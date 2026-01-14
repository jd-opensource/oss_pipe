use super::transfer_local2local::TransferLocal2Local;
use super::transfer_local2oss::TransferLocal2Oss;
use super::transfer_oss2local::TransferOss2Local;
use super::transfer_oss2oss::TransferOss2Oss;
use crate::checkpoint::RecordOption;
use crate::commons::prompt_processbar;
use crate::commons::{count_file_bytes, count_file_lines, json_to_struct};
use crate::consts::task_consts::{
    NOTIFY_FILE_PREFIX, OBJECTS_SEQUENCE_FILE, OFFSET_PREFIX, TRANSFER_CHECK_POINT_FILE,
};
use crate::models::model_checkpoint::{CheckPoint, FileDescription, FilePosition};
use crate::models::model_task::{ObjectStorage, TaskStage};
use crate::models::model_task_transfer::TransferTask;
use crate::tasks::{gen_file_path, task_traits::TransferTaskActions, TaskStatusSaver};
use crate::{
    checkpoint::{get_task_checkpoint, ListedRecord},
    commons::quantify_processbar,
};
use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use rust_decimal::prelude::*;
use std::path::Path;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Seek, SeekFrom},
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tabled::{builder::Builder, settings::Style};
use tokio::{
    sync::Semaphore,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct TransferSharedMarks {
    pub execute_stop_mark: Arc<AtomicBool>,
    pub notify_stop_mark: Arc<AtomicBool>,
    pub task_err_occur: Arc<AtomicBool>,
    pub multi_part_semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
}

impl TransferTask {
    pub async fn analyze(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        let map = task.analyze_source().await?;
        let mut total_objects = 0;
        for v in map.iter() {
            total_objects += *v.value();
        }
        let total = Decimal::from(total_objects);

        let mut builder = Builder::default();
        let header = vec!["size_range", "objects", "percent", "total"];
        builder.insert_record(0, header);

        for kv in map.iter() {
            let val_dec = Decimal::from(*kv.value());
            if val_dec.is_zero() {
                continue;
            }
            let key = kv.key().to_string();
            let val = kv.value().to_string();
            let percent = (val_dec / total).round_dp(2).to_string();
            let raw = vec![key, val, percent, "".to_string()];
            builder.push_record(raw);
        }

        builder.insert_record(
            builder.count_records(),
            vec![
                "".to_string(),
                "".to_string(),
                "".to_string(),
                total_objects.to_string(),
            ],
        );

        let mut table = builder.build();
        table.with(Style::ascii_rounded());
        println!("{}", table);

        Ok(())
    }

    pub async fn start_transfer(&self) -> Result<()> {
        /// ToDo 重构任务执行结构
        /// 三个阶段 init、stock、increment
        /// 1. init 阶段：初始化任务，检查任务状态，恢复任务进度，创建任务检查点文件
        /// 2. stock 阶段：存量数据迁移，迁移存量数据，迁移完成后，更新任务检查点文件
        /// 3. increment 阶段：增量数据迁移，迁移增量数据，迁移完成后，更新任务检查点文件
        // sys_set 用于执行 checkpoint、notify 等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut exec_set = JoinSet::new();
        let checkpoint_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        let shared_marks = TransferSharedMarks {
            execute_stop_mark: Arc::new(AtomicBool::new(false)),
            notify_stop_mark: Arc::new(AtomicBool::new(false)),
            task_err_occur: Arc::new(AtomicBool::new(false)),
            multi_part_semaphore: Arc::new(Semaphore::new(
                self.attributes.multi_part_max_parallelism,
            )),
            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
        };

        // let mut assistant = IncrementAssistant::default();
        // assistant.check_point_path = checkpoint_file.clone();
        // let increment_assistant = Arc::new(Mutex::new(assistant));

        // 任务启动前清理未执行完成的 target 端 multi part
        self.target.oss_clean_multi_parts().await?;

        // 判断是否执行任务初始化
        if !self.attributes.start_from_checkpoint {
            self.init_task().await?;
        }

        let checkpoint = get_task_checkpoint(&checkpoint_file)?;

        // 从自定义列表同步，只同步列表中的 key
        if let Some(lists) = &self.attributes.objects_list_files {
            // 获取文件列表以及当前执行位置
            // let obj_list_files = self.get_specified_list_files_and_position(lists)?;
            let obj_list_files = self.get_specified_objects_list_files_desc(lists)?;
            if obj_list_files.len() > 0 {
                self.execute_stock_transfer(
                    &mut sys_set,
                    &mut exec_set,
                    shared_marks,
                    checkpoint_file.clone(),
                    obj_list_files,
                    None,
                )
                .await
                .context(format!("{}:{}", file!(), line!()))?;
            }
            // 执行文件列表
            return Ok(());
        }

        //获取 对象列表文件，列表文件描述，increame_start_from_checkpoint 标识
        // let (
        //     object_list_file,
        //     obj_list_files,
        //     list_file_position,
        //     start_from_checkpoint_stage_is_increment,
        // ) = self
        //     .get_list_files_and_position()
        //     .await
        //     .context(format!("{}:{}", file!(), line!()))?;
        let obj_list_files =
            self.get_objects_list_files_desc()
                .context(format!("{}:{}", file!(), line!()))?;

        // 全量同步时：执行增量助理
        let mut notify_file_path = None;

        // 增量或全量同步时，提前启动 increment_assistant 用于记录 notify 文件
        if self.attributes.transfer_type.is_full() || self.attributes.transfer_type.is_increment() {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
            notify_file_path = Some(gen_file_path(
                self.attributes.meta_dir.as_str(),
                NOTIFY_FILE_PREFIX,
                now.as_secs().to_string().as_str(),
            ));
            self.increment_prelude(
                &mut sys_set,
                shared_marks.notify_stop_mark.clone(),
                shared_marks.task_err_occur.clone(),
                &mut notify_file_path,
                // increment_assistant.clone(),
            )
            .await;
        }

        // 根据 stage 判断是否执行存量数据迁移
        if matches!(checkpoint.task_stage, TaskStage::Stock) {
            // 存量数据迁移完成，执行增量数据迁移
            self.execute_stock_transfer(
                &mut sys_set,
                &mut exec_set,
                shared_marks.clone(),
                checkpoint_file.clone(),
                obj_list_files.clone(),
                notify_file_path.clone(),
            )
            .await?;
        }

        while exec_set.len() > 0 {
            exec_set.join_next().await;
        }

        // 配置停止 offset save 标识为 true
        shared_marks
            .execute_stop_mark
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // sleep 时长为 TaskStatusSaver interval 的两倍，保证线程结束
        tokio::time::sleep(tokio::time::Duration::from_secs(6)).await;

        // 判断存量任务是否正常结束
        if shared_marks
            .task_err_occur
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            shared_marks
                .notify_stop_mark
                .store(true, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            return Err(anyhow!("error occur")).context(format!("{}:{}", file!(), line!()));
        }

        // 将对象文件列表最后一个文件的信息写入 checkpoint，代表存量任务结束
        if let Some(file_desc) = obj_list_files.last() {
            let mut checkpoint = get_task_checkpoint(checkpoint_file.as_str())?;
            checkpoint.executing_file = file_desc.clone();
            checkpoint.executing_file_position.offset = file_desc.size.try_into()?;
            checkpoint.executing_file_position.line_num = file_desc.total_lines;
            checkpoint.save_to(&checkpoint_file)?;
        }

        // 准备增量任务
        let mut checkpoint = get_task_checkpoint(checkpoint_file.as_str())?;

        checkpoint.file_for_notify = notify_file_path.clone();
        // 设置存量文件 executed_file_position 为文件执行完成的位置

        if let Err(e) = checkpoint.save_to(checkpoint_file.as_str()) {
            log::error!("{:?}", e);
        };

        // 执行增量逻辑
        if self.attributes.transfer_type.is_full() || self.attributes.transfer_type.is_increment() {
            //更新 checkpoint 更改 stage
            let mut checkpoint = get_task_checkpoint(checkpoint_file.as_str())?;

            if !matches!(checkpoint.task_stage, TaskStage::Increment) {
                checkpoint.task_stage = TaskStage::Increment;
                if let Err(e) = checkpoint.save_to(checkpoint_file.as_str()) {
                    log::error!("{:?}", e);
                };
            }

            shared_marks
                .execute_stop_mark
                .store(false, std::sync::atomic::Ordering::Relaxed);

            self.execute_increment_transfer(
                &mut exec_set,
                shared_marks.clone(),
                &checkpoint_file,
                notify_file_path,
                // increment_assistant,
            )
            .await?;
        }

        while sys_set.len() > 0 {
            task::yield_now().await;
            sys_set.join_next().await;
        }

        //判断任务是否异常退出
        if shared_marks
            .task_err_occur
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(anyhow!("task error occur"));
        }

        Ok(())
    }

    /// 初始化传输任务
    ///
    /// 该方法负责传输任务的初始化工作，主要包括：
    /// 1. 清理并重新生成对象列表文件
    /// 2. 创建初始检查点文件
    /// 3. 显示对象列表文件的统计信息
    ///
    /// # 功能详述
    /// - 清理 meta 目录中的旧文件
    /// - 根据源存储生成多个对象列表文件
    /// - 创建包含任务开始时间戳的检查点文件
    /// - 以表格形式展示生成的列表文件信息（路径、大小、行数）
    ///
    /// # 返回值
    /// - `Ok(())`: 初始化成功
    /// - `Err(anyhow::Error)`: 初始化过程中发生错误
    ///
    /// # 错误情况
    /// - 无法获取系统时间戳
    /// - 生成对象列表文件失败
    /// - 保存检查点文件失败
    pub async fn init_task(&self) -> Result<()> {
        let task = self.gen_transfer_actions();
        log::info!("generate objects list files beging");
        let pd = prompt_processbar("Generating object list ...");

        // 清理 meta 目录
        // 重新生成 object list file
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(format!("{}:{}", file!(), line!()))?;

        let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());

        let list_files = task
            .list_objects_to_multi_files(&self.attributes.meta_dir)
            .await
            .context(format!("{}:{}", file!(), line!()))?;

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        let mut checkpoint = CheckPoint::default();
        checkpoint.executing_file = list_files[0].clone();
        checkpoint.task_begin_timestamp = i128::from(now.as_secs());
        checkpoint.modify_checkpoint_timestamp = i128::from(now.as_secs());
        checkpoint.last_scan_timestamp = i128::from(now.as_secs());

        checkpoint
            .save_to(check_point_file.as_str())
            .context(format!("{}:{}", file!(), line!()))?;

        pd.finish_with_message("object list generated");

        let mut builder = Builder::default();
        let header = vec!["path", "size", "total_lines"];
        builder.insert_record(0, header);

        for f in list_files.iter() {
            let raw = vec![
                f.path.to_string(),
                f.size.to_string(),
                f.total_lines.to_string(),
            ];
            builder.push_record(raw);
        }

        builder.insert_record(
            builder.count_records(),
            vec!["".to_string(), "".to_string(), "".to_string()],
        );

        let mut table = builder.build();
        table.with(Style::ascii_rounded());
        println!("{}", table);
        log::info!("generate objects list files ok");

        Ok(())
    }

    /// 增量传输前置操作
    ///
    /// 此函数负责启动增量传输前的准备工作，包括：
    /// 1. 启动增量前置任务，用于监听本地目录变化或记录时间戳
    /// 2. 当源存储为本地存储时，获取 notify 文件路径用于监听文件变化
    /// 3. 启动心跳任务，确保 notify 机制能够正常工作和结束
    ///
    /// # 参数
    /// * `sys_set` - 系统任务集合，用于管理后台任务
    /// * `notify_stop_mark` - 停止标记，用于控制任务停止
    /// * `err_occur` - 错误发生标记，用于标识是否有错误发生
    /// * `file_for_notify` - notify 文件路径，用于存储监听文件的路径
    /// * `increment_assistant` - 增量助手，用于管理增量传输相关状态
    async fn increment_prelude(
        &self,
        sys_set: &mut JoinSet<()>,
        notify_stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        file_for_notify: &mut Option<String>,
        // increment_assistant: Arc<Mutex<IncrementAssistant>>,
    ) {
        let task_increment_prelude = self.gen_transfer_actions();
        // let assistant = Arc::clone(&increment_assistant);
        let n_s_m = notify_stop_mark.clone();
        let e_o = err_occur.clone();

        // 启动 increment_prelude 监听增量时的本地目录，或记录间戳
        let notify = file_for_notify.clone();
        // sys_set.spawn(async move {
        //     if let Err(e) = task_increment_prelude
        //         .increment_prelude(n_s_m, e_o, notify, assistant)
        //         .await
        //     {
        //         log::error!("{:?}", e);
        //         return;
        //     }
        // });
        sys_set.spawn(async move {
            if let Err(e) = task_increment_prelude
                .increment_prelude(n_s_m, e_o, notify)
                .await
            {
                log::error!("{:?}", e);
                return;
            }
        });
        // 当源存储为本地时，获取 notify 文件
        if let ObjectStorage::Local(dir) = self.source.clone() {
            // while file_for_notify.is_none() {
            //     let lock = increment_assistant.lock().await;
            //     *file_for_notify = lock.get_notify_file_path();
            //     drop(lock);
            //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            // }

            // 启动心跳，notify 需要有 event 输入才能有效结束任务
            let n_s_m = notify_stop_mark.clone();
            let e_o = err_occur.clone();
            sys_set.spawn(async move {
                let tmp_file = gen_file_path(dir.as_str(), "oss_pipe_tmp", "");
                while !e_o.load(std::sync::atomic::Ordering::SeqCst)
                    && !n_s_m.load(std::sync::atomic::Ordering::SeqCst)
                {
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    task::yield_now().await;
                }
                let f = OpenOptions::new()
                    .truncate(true)
                    .create(true)
                    .write(true)
                    .open(tmp_file.as_str())
                    .unwrap();
                drop(f);
                let _ = fs::remove_file(tmp_file);
            });
        }
    }

    async fn execute_stock_transfer(
        &self,
        sys_set: &mut JoinSet<()>,
        exec_set: &mut JoinSet<()>,
        shared_marks: TransferSharedMarks,
        check_point_file: String,
        obj_list_files: Vec<FileDescription>,
        file_for_notify: Option<String>,
    ) -> Result<()> {
        let check_point = get_task_checkpoint(&check_point_file)?;
        // 启动 checkpoint 记录线程
        let stock_status_saver = TaskStatusSaver {
            check_point_path: check_point_file.clone(),
            executed_file: check_point.executing_file,
            stop_mark: Arc::clone(&shared_marks.execute_stop_mark),
            list_file_positon_map: shared_marks.offset_map.clone(),
            file_for_notify,
            task_stage: TaskStage::Stock,
            interval: 3,
            object_list_files: Some(obj_list_files.clone()),
        };
        let task_id = self.task_id.clone();
        sys_set.spawn(async move {
            stock_status_saver.snapshot_to_file(task_id).await;
        });

        // 启动进度条线程
        let bar_stop_mark = shared_marks.execute_stop_mark.clone();
        let total = obj_list_files.iter().map(|f| f.total_lines).sum();
        let cp = check_point_file.clone();
        let list_files = obj_list_files.clone();
        sys_set.spawn(async move {
            // Todo 调整进度条
            quantify_processbar(total, bar_stop_mark, &cp, Some(list_files)).await;
        });

        // multi_files
        self.transfer_records_from_multi_files(
            exec_set,
            shared_marks.clone(),
            check_point.executing_file_position,
            obj_list_files.clone(),
        )
        .await
    }

    async fn execute_increment_transfer(
        &self,
        exec_set: &mut JoinSet<()>,
        transfer_shared_marks: TransferSharedMarks,
        checkpoint_file: &str,
        file_for_notify: Option<String>,
        // increment_assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()> {
        let mut checkpoint = get_task_checkpoint(&checkpoint_file)?;
        checkpoint.task_stage = TaskStage::Increment;
        checkpoint.save_to(&checkpoint_file)?;

        let task_increment = self.gen_transfer_actions();

        task_increment
            .execute_increment(
                exec_set,
                transfer_shared_marks.execute_stop_mark.clone(),
                transfer_shared_marks.task_err_occur.clone(),
                transfer_shared_marks.multi_part_semaphore.clone(),
                transfer_shared_marks.offset_map.clone(),
                &checkpoint_file,
                file_for_notify,
                // Arc::clone(&increment_assistant),
                self.attributes.increment_mode,
            )
            .await;
        Ok(())
    }
}

impl TransferTask {
    pub fn gen_transfer_actions(&self) -> Arc<dyn TransferTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferLocal2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferLocal2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TransferOss2Local {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TransferOss2Oss {
                        task_id: self.task_id.clone(),
                        name: self.name.clone(),
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
        }
    }

    // 获取自定义列表文件以及当前执行位置
    // fn get_specified_list_files_and_position(
    //     &self,
    //     files_path: &Vec<String>,
    // ) -> Result<Vec<FileDescription>> {
    //     return match self.attributes.start_from_checkpoint {
    //         true => {
    //             // 对象文件列表
    //             let obj_list_files_desc = self
    //                 .get_specified_objects_list_files_desc(files_path)
    //                 .context(format!("{}:{}", file!(), line!()))?;

    //             Ok(obj_list_files_desc)
    //         }
    //         false => {
    //             let obj_list_files_desc = self
    //                 .get_specified_objects_list_files_desc(files_path)
    //                 .context(format!("{}:{}", file!(), line!()))?;
    //             Ok(obj_list_files_desc)
    //         }
    //     };
    // }

    pub async fn list_objects_to_files(&self, files_folder: &str) -> Result<()> {
        let task = self.gen_transfer_actions();

        log::info!("list objects to files beging");
        let pd = prompt_processbar("Generating object list ...");
        // 清理 meta 目录
        let _ = fs::remove_dir_all(files_folder);

        let list_files = task
            .list_objects_to_multi_files(files_folder)
            .await
            .context(format!("{}:{}", file!(), line!()))?;

        pd.finish_with_message("object list generated");

        let mut builder = Builder::default();
        let header = vec!["path", "size", "total_lines"];
        builder.insert_record(0, header);

        for f in list_files.iter() {
            let raw = vec![
                f.path.to_string(),
                f.size.to_string(),
                f.total_lines.to_string(),
            ];
            builder.push_record(raw);
        }

        builder.insert_record(
            builder.count_records(),
            vec!["".to_string(), "".to_string(), "".to_string()],
        );

        let mut table = builder.build();
        table.with(Style::ascii_rounded());
        println!("{}", table);
        log::info!("list objects to files ok");

        Ok(())
    }

    pub fn get_specified_objects_list_files_desc(
        &self,
        files_path: &Vec<String>,
    ) -> Result<Vec<FileDescription>> {
        let mut record_list_files = vec![];

        for fp in files_path {
            let path = Path::new(fp);
            match path.is_dir() {
                true => {
                    // 遍历目录并将文件路径写入文件
                    for entry in WalkDir::new(path)
                        .into_iter()
                        .filter_map(Result::ok)
                        .filter(|e| !e.file_type().is_dir())
                    {
                        if let Some(p) = entry.path().to_str() {
                            // 不包含目录本身
                            // if path.eq(Path::new(p)) {
                            //     continue;
                            // }
                            let file_size =
                                count_file_bytes(p).context(format!("{}:{}", file!(), line!()))?;
                            let file_lines =
                                count_file_lines(p).context(format!("{}:{}", file!(), line!()))?;

                            let obj_list_file = FileDescription {
                                path: p.to_string(),
                                size: file_size,
                                total_lines: file_lines,
                            };
                            record_list_files.push(obj_list_file);
                        };
                    }
                }
                false => {
                    let file_size =
                        count_file_bytes(path).context(format!("{}:{}", file!(), line!()))?;
                    let file_lines =
                        count_file_lines(path).context(format!("{}:{}", file!(), line!()))?;

                    let obj_list_file = FileDescription {
                        path: fp.to_string(),
                        size: file_size,
                        total_lines: file_lines,
                    };
                    record_list_files.push(obj_list_file);
                }
            }
        }
        Ok(record_list_files)
    }

    // 根据 sequence_file 获取所有对象列表的描述列表
    pub fn get_objects_list_files_desc(&self) -> Result<Vec<FileDescription>> {
        let sequence_file_path =
            gen_file_path(&self.attributes.meta_dir, OBJECTS_SEQUENCE_FILE, "");
        let sequence_file =
            File::open(sequence_file_path).context(format!("{}:{}", file!(), line!()))?;
        let sequence_reader = BufReader::new(sequence_file);
        let record_list_files: Vec<FileDescription> = sequence_reader
            .lines()
            .map(|line| {
                let file_path = line.context(format!("{}:{}", file!(), line!()))?;
                let file_size = count_file_bytes(file_path.as_str()).context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;
                let file_lines = count_file_lines(file_path.as_str()).context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;

                let obj_list_file = FileDescription {
                    path: file_path,
                    size: file_size,
                    total_lines: file_lines,
                };
                Ok(obj_list_file)
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()
            .context(format!("{}:{}", file!(), line!()))?;
        Ok(record_list_files)
    }

    // 从多个文件列表执行传输
    async fn transfer_records_from_multi_files(
        &self,
        exec_set: &mut JoinSet<()>,
        shared_marks: TransferSharedMarks,
        file_position: FilePosition,
        obj_list_files: Vec<FileDescription>,
    ) -> Result<()> {
        let task_stock = self.gen_transfer_actions();
        let mut vec_keys: Vec<ListedRecord> = vec![];

        let mut file_num = file_position.file_num;

        // let file_num_usize = match TryInto::<usize>::try_into(file_position.file_num) {
        //     Ok(n) => n,
        //     Err(e) => {
        //         log::error!("{:?},file position:{:?}", e, file_position);
        //         stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
        //         return;
        //     }
        // };

        let file_num_usize = TryInto::<usize>::try_into(file_position.file_num)?;

        let mut file_position = file_position.clone();

        for exec_file_desc in obj_list_files.iter().skip(file_num_usize) {
            // 按 file_position seek file
            let mut exec_file = File::open(exec_file_desc.path.as_str())
                .map_err(|e| {
                    shared_marks
                        .task_err_occur
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    shared_marks
                        .execute_stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    e
                }) // ? 继续
                .context(format!("{}:{}", file!(), line!()))?;

            let seek_offset = TryInto::<u64>::try_into(file_position.offset)
                .map_err(|e| {
                    shared_marks
                        .task_err_occur
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    shared_marks
                        .execute_stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    e
                }) // ? 继续
                .context(format!("{}:{}", file!(), line!()))?;
            exec_file
                .seek(SeekFrom::Start(seek_offset))
                .map_err(|e| {
                    shared_marks
                        .task_err_occur
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    shared_marks
                        .execute_stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    e
                }) // ? 继续
                .context(format!("{}:{}", file!(), line!()))?;

            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(exec_file).lines();
            let exec_lines = exec_file_desc.total_lines - file_position.line_num;

            for (line_idx, line) in lines.enumerate() {
                // 若错误达到上限，则停止任务
                if shared_marks
                    .execute_stop_mark
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    break;
                }

                match line {
                    Ok(key) => {
                        // 先写入当前 key 开头的 offset，然后更新 list_file_position 作为下一个 key 的 offset，待验证效果
                        let len = key.bytes().len() + "\n".bytes().len();

                        // #[cfg(target_family = "unix")]
                        if !key.ends_with("/") {
                            let record = ListedRecord {
                                file_num,
                                key,
                                offset: file_position.offset,
                                line_num: file_position.line_num,
                            };
                            vec_keys.push(record);
                        }

                        // #[cfg(target_family = "windows")]
                        // if !key.ends_with("\\") {
                        //     let record = ListedRecord {
                        //         key,
                        //         offset: file_position.offset,
                        //         line_num: file_position.line_num,
                        //     };
                        //     vec_keys.push(record);
                        // }

                        file_position.offset += len;
                        file_position.line_num += 1;
                    }
                    Err(e) => {
                        log::error!("{:?},file position:{:?}", e, file_position);
                        shared_marks
                            .execute_stop_mark
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                }

                // 批量传输条件：临时队列达到批量值，执行到每一文件的最后一行
                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_transfer_batch.to_string())
                    || line_idx.eq(&TryInto::<usize>::try_into(exec_lines - 1).unwrap())
                        && vec_keys.len() > 0
                {
                    while exec_set.len() >= self.attributes.task_parallelism {
                        exec_set.join_next().await;
                    }

                    // 提前插入 offset 保证顺序性
                    let mut offset_key: String = OFFSET_PREFIX.to_string();
                    let subffix =
                        vec_keys[0].file_num.to_string() + "_" + &vec_keys[0].offset.to_string();
                    offset_key.push_str(&subffix);

                    shared_marks.offset_map.insert(
                        offset_key.clone(),
                        FilePosition {
                            file_num: vec_keys[0].file_num,
                            offset: vec_keys[0].offset,
                            line_num: vec_keys[0].line_num,
                        },
                    );

                    let vk = vec_keys.clone();

                    // Todo
                    // 验证 gen_transfer_executor 使用 exec_set.spawn
                    let record_executer = task_stock.gen_transfer_executor(
                        shared_marks.execute_stop_mark.clone(),
                        shared_marks.task_err_occur.clone(),
                        shared_marks.multi_part_semaphore.clone(),
                        shared_marks.offset_map.clone(),
                        exec_file_desc.path.to_string(),
                    );
                    let eo = shared_marks.task_err_occur.clone();
                    let sm = shared_marks.execute_stop_mark.clone();

                    exec_set.spawn(async move {
                        if let Err(e) = record_executer.transfer_listed_records(vk).await {
                            eo.store(true, std::sync::atomic::Ordering::SeqCst);
                            sm.store(true, std::sync::atomic::Ordering::SeqCst);
                            log::error!("{:?}", e);
                        };
                    });

                    // 清理临时 key vec
                    vec_keys.clear();
                }
            }
            file_num += 1;
            file_position = FilePosition {
                file_num,
                offset: 0,
                line_num: 0,
            };
        }
        Ok(())
    }

    // 增量场景下执行 record_descriptions 文件
    async fn exec_record_descriptions_file(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        exec_set: &mut JoinSet<()>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        records_desc_file: File,
        executing_file: FileDescription,
    ) {
        let task_modify = self.gen_transfer_actions();
        let mut vec_keys: Vec<RecordOption> = vec![];

        // 按列表传输 object from source to target
        let total_lines = executing_file.total_lines;
        let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(records_desc_file).lines();

        for (idx, line) in lines.enumerate() {
            // 若错误达到上限，则停止任务
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            if let Result::Ok(l) = line {
                let record = match json_to_struct::<RecordOption>(&l) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("{:?}", e);
                        err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                        stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                };
                vec_keys.push(record);
            };

            if vec_keys
                .len()
                .to_string()
                .eq(&self.attributes.objects_transfer_batch.to_string())
                || idx.to_string().eq(&(total_lines - 1).to_string()) && vec_keys.len() > 0
            {
                while exec_set.len() >= self.attributes.task_parallelism {
                    exec_set.join_next().await;
                }

                let vk = vec_keys.clone();
                let record_executer = task_modify.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    semaphore.clone(),
                    offset_map.clone(),
                    executing_file.path.to_string(),
                );

                exec_set.spawn(async move {
                    if let Err(e) = record_executer.transfer_record_options(vk).await {
                        log::error!("{:?}", e);
                    };
                });

                // 清理临时 key vec
                vec_keys.clear();
            }
        }
    }
}
