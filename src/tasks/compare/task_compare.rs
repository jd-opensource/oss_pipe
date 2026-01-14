use super::{
    compare_local2local::TaskCompareLocal2Local, compare_local2oss::TaskCompareLocal2Oss,
    compare_oss2local::TaskCompareOss2Local, compare_oss2oss::TaskCompareOss2Oss,
};
use crate::commons::{count_file_bytes, count_file_lines};
use crate::consts::task_consts::{
    COMPARE_CHECK_POINT_FILE, COMPARE_RESULT_PREFIX, OBJECTS_SEQUENCE_FILE, OFFSET_PREFIX,
};
use crate::models::model_checkpoint::{CheckPoint, FileDescription, FilePosition};
use crate::models::model_task::{ObjectStorage, TaskStage};
use crate::models::model_task_compare::CompareTask;

use crate::{
    checkpoint::{get_task_checkpoint, ListedRecord},
    commons::{json_to_struct, prompt_processbar, quantify_processbar},
    tasks::{gen_file_path, task_traits::CompareTaskActions, TaskStatusSaver},
};
use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::io::{self, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    fmt::{self},
    fs::{self, File},
    io::{BufRead, BufReader, Lines, Write},
    sync::{atomic::AtomicBool, Arc},
};
use tabled::builder::Builder;
use tabled::settings::Style;
use tokio::task;
use tokio::{sync::Semaphore, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
// 定义一个结构体，用于存储对象差异
pub struct ObjectDiff {
    // 源对象
    pub source: String,
    // 目标对象
    pub target: String,
    // 对象差异
    pub diff: Diff,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Diff {
    ExistsDiff(DiffExists),
    LengthDiff(DiffLength),
    ExpiresDiff(DiffExpires),
    ContentDiff(DiffContent),
    MetaDiff(DiffMeta),
}

impl Diff {
    pub fn name(&self) -> String {
        match self {
            Diff::ExistsDiff(_) => "exists_diff".to_string(),
            Diff::LengthDiff(_) => "length_diff".to_string(),
            Diff::ExpiresDiff(_) => "exprires_diff".to_string(),
            Diff::ContentDiff(_) => "content_diff".to_string(),
            Diff::MetaDiff(_) => "meta_data_diff".to_string(),
        }
    }
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Diff::ExistsDiff(d) => {
                write!(f, "{};{}", d.source_exists, d.target_exists)
            }
            Diff::LengthDiff(d) => {
                write!(f, "{};{}", d.source_content_len, d.target_content_len)
            }
            Diff::ExpiresDiff(d) => {
                write!(f, "{:?};{:?}", d.source_expires, d.target_expires)
            }
            Diff::ContentDiff(d) => {
                write!(
                    f,
                    "{};{};{}",
                    d.stream_position, d.source_byte, d.target_byte
                )
            }
            Diff::MetaDiff(d) => {
                write!(f, "{:?};{:?}", d.source_meta, d.target_meta)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExists {
    pub source_exists: bool,
    pub target_exists: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffExpires {
    pub source_expires: Option<DateTime>,
    pub target_expires: Option<DateTime>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateTime {
    pub seconds: i64,
    pub subsecond_nanos: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffLength {
    pub source_content_len: i128,
    pub target_content_len: i128,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffContent {
    pub stream_position: usize,
    pub source_byte: u8,
    pub target_byte: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffMeta {
    pub source_meta: Option<HashMap<std::string::String, std::string::String>>,
    pub target_meta: Option<HashMap<std::string::String, std::string::String>>,
}

impl ObjectDiff {
    pub fn save_json_to_file(&self, file: &mut File) -> Result<()> {
        // 获取文件路径，若不存在则创建路径
        let mut json = serde_json::to_string(self)?;
        json.push_str("\n");
        file.write_all(json.as_bytes())?;
        file.flush()?;
        Ok(())
    }
}

impl CompareTask {
    pub fn gen_compare_actions(&self) -> Arc<dyn CompareTaskActions + Send + Sync> {
        match &self.source {
            ObjectStorage::Local(path_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TaskCompareLocal2Local {
                        source: path_s.to_string(),
                        target: path_t.to_string(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TaskCompareLocal2Oss {
                        source: path_s.to_string(),
                        target: oss_t.clone(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
            ObjectStorage::OSS(oss_s) => match &self.target {
                ObjectStorage::Local(path_t) => {
                    let t = TaskCompareOss2Local {
                        source: oss_s.clone(),
                        target: path_t.to_string(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
                ObjectStorage::OSS(oss_t) => {
                    let t = TaskCompareOss2Oss {
                        source: oss_s.clone(),
                        target: oss_t.clone(),
                        check_option: self.check_option.clone(),
                        attributes: self.attributes.clone(),
                    };
                    Arc::new(t)
                }
            },
        }
    }

    pub async fn start_compare(&self) -> Result<()> {
        // sys_set 用于执行 checkpoint、notify 等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut exec_set = JoinSet::new();

        let task_stop_mark = Arc::new(AtomicBool::new(false));
        let task_err_occur = Arc::new(AtomicBool::new(false));
        let task_multi_part_semaphore =
            Arc::new(Semaphore::new(self.attributes.multi_part_max_parallelism));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            COMPARE_CHECK_POINT_FILE,
            "",
        );

        let (obj_list_files, mut current_list_file_position) = self
            .get_list_files_and_position()
            .await
            .context(format!("{}:{}", file!(), line!()))?;

        let stock_status_saver = TaskStatusSaver {
            check_point_path: check_point_file.clone(),
            executed_file: obj_list_files[0].clone(),
            stop_mark: Arc::clone(&task_stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: None,
            task_stage: TaskStage::Stock,
            interval: 3,
            object_list_files: Some(obj_list_files.clone()),
        };
        let task_id = self.task_id.clone();
        sys_set.spawn(async move {
            stock_status_saver.snapshot_to_file(task_id).await;
        });

        // 启动进度条线程
        let stop_mark = Arc::clone(&task_stop_mark);
        let total = obj_list_files.iter().map(|f| f.total_lines).sum();
        let cp = check_point_file.clone();
        sys_set.spawn(async move {
            quantify_processbar(total, stop_mark.clone(), &cp, None).await;
        });

        let stop_mark = Arc::clone(&task_stop_mark);
        self.compare_records_multi_files(
            stop_mark.clone(),
            task_err_occur.clone(),
            task_multi_part_semaphore.clone(),
            &mut exec_set,
            offset_map.clone(),
            &mut current_list_file_position,
            // record_list_files,
            obj_list_files.clone(),
        )
        .await;

        while exec_set.len() > 0 {
            exec_set.join_next().await;
        }
        // 配置停止 offset save 标识为 true
        stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);

        while sys_set.len() > 0 {
            task::yield_now().await;
            sys_set.join_next().await;
        }

        if task_err_occur.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("compare task error"));
        }

        for entry in WalkDir::new(&self.attributes.meta_dir)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.attributes.meta_dir) {
                    continue;
                }

                let key = match &self.attributes.meta_dir.ends_with("/") {
                    true => &p[self.attributes.meta_dir.len()..],
                    false => &p[self.attributes.meta_dir.len() + 1..],
                };

                if key.starts_with(&COMPARE_RESULT_PREFIX) {
                    let result_file = gen_file_path(&self.attributes.meta_dir, key, "");
                    let _ = show_compare_result(&result_file);
                }
            };
        }
        Ok(())
    }

    async fn compare_records_multi_files(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        multi_parts_semaphore: Arc<Semaphore>,
        exec_set: &mut JoinSet<()>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        file_position: &mut FilePosition,
        obj_list_files: Vec<FileDescription>,
    ) {
        let compare = self.gen_compare_actions();
        let mut vec_keys: Vec<ListedRecord> = vec![];

        let mut file_num = file_position.file_num;

        let file_num_usize = match TryInto::<usize>::try_into(file_position.file_num) {
            Ok(n) => n,
            Err(e) => {
                log::error!("{:?},file position:{:?}", e, file_position);
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        };

        let mut file_position = file_position.clone();

        // let file_executed_lines = file_position.line_num;
        for exec_file_desc in obj_list_files.iter().skip(file_num_usize) {
            // 按 file_position seek file
            let mut exec_file = File::open(exec_file_desc.path.as_str())
                .context(format!("{}:{}", file!(), line!()))
                .unwrap();
            let seek_offset = TryInto::<u64>::try_into(file_position.offset)
                .context(format!("{}:{}", file!(), line!()))
                .unwrap();
            exec_file
                .seek(SeekFrom::Start(seek_offset))
                .context(format!("{}:{}", file!(), line!()))
                .unwrap();

            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(exec_file).lines();
            let exec_lines = exec_file_desc.total_lines - file_position.line_num;

            for (line_idx, line) in lines.enumerate() {
                // 若错误达到上限，则停止任务
                if stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
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
                        stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                }

                // 批量传输条件：临时队列达到批量值，执行到每一文件的最后一行
                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_per_batch.to_string())
                    || line_idx.eq(&TryInto::<usize>::try_into(exec_lines - 1).unwrap())
                        && vec_keys.len() > 0
                {
                    while exec_set.len() >= self.attributes.task_parallelism {
                        exec_set.join_next().await;
                    }

                    // 提前插入 offset 保证顺序性
                    let mut offset_key = OFFSET_PREFIX.to_string();
                    let subffix =
                        vec_keys[0].file_num.to_string() + "_" + &vec_keys[0].offset.to_string();
                    offset_key.push_str(&subffix);

                    offset_map.insert(
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
                    let record_executer = compare.gen_compare_executor(
                        stop_mark.clone(),
                        err_occur.clone(),
                        multi_parts_semaphore.clone(),
                        offset_map.clone(),
                    );
                    let eo = err_occur.clone();
                    let sm = stop_mark.clone();

                    exec_set.spawn(async move {
                        let mut offset_key = OFFSET_PREFIX.to_string();
                        let subffix = vk[0].file_num.to_string() + "_" + &vk[0].offset.to_string();
                        offset_key.push_str(&subffix);
                        if let Err(e) = record_executer.compare_listed_records(vk).await {
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
    }

    async fn get_list_files_and_position(
        &self,
        // task: Arc<dyn CompareTaskActions + Send + Sync>,
    ) -> Result<(Vec<FileDescription>, FilePosition)> {
        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            COMPARE_CHECK_POINT_FILE,
            "",
        );
        return match self.attributes.start_from_checkpoint {
            true => {
                let checkpoint = get_task_checkpoint(check_point_file.as_str())
                    .context(format!("{}:{}", file!(), line!()))?;
                let list_files_desc = self.gen_source_object_list_file_desc().context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;
                let list_file_position = checkpoint.executing_file_position.clone();
                Ok((list_files_desc, list_file_position))
            }
            false => {
                self.init_task()
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;
                let list_files_desc = self.gen_source_object_list_file_desc().context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;
                let checkpoint = get_task_checkpoint(check_point_file.as_str())
                    .context(format!("{}:{}", file!(), line!()))?;
                let list_file_position = checkpoint.executing_file_position.clone();
                Ok((list_files_desc, list_file_position))
            }
        };
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
        let task = self.gen_compare_actions();

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
            COMPARE_CHECK_POINT_FILE,
            "",
        );

        let mut checkpoint = CheckPoint::default();
        checkpoint.executing_file = list_files[0].clone();
        checkpoint.task_begin_timestamp = i128::from(now.as_secs());
        checkpoint.modify_checkpoint_timestamp = i128::from(now.as_secs());

        checkpoint.save_to(check_point_file.as_str())?;

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

    // 根据 sequence_file 获取所有对象列表的描述列表
    pub fn gen_source_object_list_file_desc(&self) -> Result<Vec<FileDescription>> {
        let sequence_file_path =
            gen_file_path(&self.attributes.meta_dir, OBJECTS_SEQUENCE_FILE, "");
        let sequence_file = File::open(sequence_file_path)?;
        let sequence_reader = BufReader::new(sequence_file);
        let record_list_files: Vec<FileDescription> = sequence_reader
            .lines()
            .map(|line| {
                let file_path = line?;

                let file_size = count_file_bytes(file_path.as_str())?;
                let file_lines = count_file_lines(file_path.as_str())?;

                let obj_list_file = FileDescription {
                    path: file_path,
                    size: file_size,
                    total_lines: file_lines,
                };
                Ok(obj_list_file)
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        Ok(record_list_files)
    }
}

pub fn show_compare_result(result_file: &str) -> Result<()> {
    let file = File::open(result_file)?;
    let lines: Lines<BufReader<File>> = BufReader::new(file).lines();
    let mut builder = Builder::default();
    for line in lines {
        if let Ok(str) = line {
            let result = json_to_struct::<ObjectDiff>(&str)?;
            let source = result.source;
            let target = result.target;
            let diff = result.diff;

            let raw = vec![source, target, diff.name(), diff.to_string()];
            builder.push_record(raw);
        }
    }

    let header = vec!["source", "target", "diff_type", "diff"];
    builder.insert_record(0, header);

    let table = builder.build();
    // table.with(Style::ascii_rounded());
    println!("{}", table);
    Ok(())
}
