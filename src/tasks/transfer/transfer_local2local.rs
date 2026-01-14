use crate::checkpoint::{get_task_checkpoint, ListedRecord, Opt, RecordOption};
use crate::commons::{
    analyze_folder_files_size, copy_file, count_file_bytes, create_parent_dir, json_to_struct,
    merge_file, prompt_processbar, read_lines, scan_folder_files_to_multi_files,
    struct_to_json_string, Modified, ModifyType, NotifyWatcher, PathType, RegexFilter,
};
use crate::consts::task_consts::{
    MODIFIED_PREFIX, OFFSET_PREFIX, REMOVED_PREFIX, TRANSFER_ERROR_RECORD_PREFIX,
};
use crate::models::model_checkpoint::{FileDescription, FilePosition};
use crate::models::model_filters::{LastModifyFilter, LastModifyFilterType};
use crate::models::model_task::TaskStage;
use crate::models::model_task_default_parameters::TaskDefaultParameters;
use crate::models::model_task_transfer::{IncrementMode, TransferTaskAttributes};
use crate::tasks::LogInfo;
use crate::tasks::{
    gen_file_path,
    task_traits::{TransferExecutor, TransferTaskActions},
    TaskStatusSaver,
};
use anyhow::Result;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::io;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;
use tokio::task::{self, JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
// 定义一个结构体 TransferLocal2Local，用于表示本地到本地的传输任务
pub struct TransferLocal2Local {
    // 任务 ID，默认值为 TaskDefaultParameters::id_default()
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    // 任务名称，默认值为 TaskDefaultParameters::name_default()
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    // 源文件路径
    pub source: String,
    // 目标文件路径
    pub target: String,
    // 传输任务属性
    pub attributes: TransferTaskAttributes,
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Local {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        analyze_folder_files_size(
            &self.source,
            regex_filter,
            self.attributes.last_modify_filter,
        )
    }

    async fn error_record_retry(
        &self,
        stop_mark: Arc<AtomicBool>,
        _semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        // 遍历 meta dir 执行所有 err 开头文件
        for entry in WalkDir::new(self.attributes.meta_dir.as_str())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir() && e.file_name().to_str().is_some())
        {
            let file_name = match entry.file_name().to_str() {
                Some(name) => name,
                None => {
                    continue;
                }
            };

            if !file_name.starts_with(TRANSFER_ERROR_RECORD_PREFIX) {
                continue;
            };

            if let Some(p) = entry.path().to_str() {
                if let Ok(lines) = read_lines(p) {
                    let mut record_vec = vec![];
                    for line in lines {
                        match line {
                            Ok(content) => {
                                let record = json_to_struct::<RecordOption>(content.as_str())?;
                                record_vec.push(record);
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                                return Err(anyhow!("{}", e));
                            }
                        }
                    }

                    if record_vec.len() > 0 {
                        let local2local = TransferLocal2LocalExecutor {
                            source: self.source.clone(),
                            target: self.target.clone(),
                            stop_mark: stop_mark.clone(),
                            err_occur: Arc::new(AtomicBool::new(false)),
                            offset_map: Arc::new(DashMap::<String, FilePosition>::new()),
                            attributes: self.attributes.clone(),
                            list_file_path: p.to_string(),
                        };
                        let _ = local2local.transfer_record_options(record_vec);
                    }
                }
                let _ = fs::remove_file(p);
            }
        }

        Ok(())
    }

    fn gen_transfer_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        _semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file_path: String,
    ) -> Arc<dyn TransferExecutor + Send + Sync> {
        let executor = TransferLocal2LocalExecutor {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark,
            err_occur,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path,
        };
        Arc::new(executor)
    }

    async fn list_objects_to_multi_files(&self, meta_dir: &str) -> Result<Vec<FileDescription>> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        scan_folder_files_to_multi_files(
            self.source.as_str(),
            regex_filter,
            self.attributes.last_modify_filter,
            meta_dir,
            self.attributes.objects_list_file_max_line,
        )
    }

    async fn changed_object_capture_based_target(
        &self,
        timestamp: usize,
    ) -> Result<FileDescription> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        // 获取 target object 列表 和 removed 列表
        // 根据时间戳生成增量列表
        // 合并删除及新增列表
        let removed = gen_file_path(
            &self.attributes.meta_dir,
            REMOVED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        let modified = gen_file_path(
            &self.attributes.meta_dir,
            MODIFIED_PREFIX,
            now.as_secs().to_string().as_str(),
        );

        self.capture_removed_objects_to_file(&removed).await?;
        self.capture_modified_objects_to_file(timestamp, &modified)
            .await?;

        merge_file(&modified, &removed, self.attributes.multi_part_chunk_size)?;
        fs::rename(&removed, &modified)?;

        let file_desc = FileDescription::from_file(&modified)?;

        Ok(file_desc)
    }

    async fn increment_prelude(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        notify_file_path: Option<String>,
        // assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()> {
        let notify_file_path = match notify_file_path {
            Some(f) => f,
            None => {
                return Err(anyhow!("notify file is none"));
            }
        };
        let watcher = NotifyWatcher::new(&self.source)?;
        let file_for_notify = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(notify_file_path.as_str())?;

        let notify_file_size = Arc::new(AtomicU64::new(0));
        watcher
            .watch_to_file(
                stop_mark,
                err_occur,
                file_for_notify,
                Arc::clone(&notify_file_size),
            )
            .await;

        Ok(())
    }

    async fn execute_increment(
        &self,
        execute_set: &mut JoinSet<()>,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        checkpoint_path: &str,
        file_for_notify: Option<String>,
        // assistant: Arc<Mutex<IncrementAssistant>>,
        increment_mod: IncrementMode,
    ) {
        let s_m = stop_mark.clone();
        let e_o = err_occur.clone();
        let r = match increment_mod {
            IncrementMode::Notify => match file_for_notify {
                Some(f) => {
                    self.increment_notify(
                        execute_set,
                        s_m,
                        e_o,
                        semaphore,
                        offset_map,
                        checkpoint_path,
                        f,
                        // assistant,
                    )
                    .await
                }
                None => Err(anyhow::anyhow!("nodtify file is none")),
            },
            IncrementMode::Scan { interval } => {
                self.increment_scan(
                    execute_set,
                    s_m,
                    e_o,
                    semaphore,
                    offset_map,
                    checkpoint_path,
                    // assistant,
                    interval,
                )
                .await
            }
        };
        match r {
            Ok(_) => {}
            Err(e) => {
                log::error!("{:?}", e);
                err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        }
        // let lock = assistant.lock().await;
        // let local_notify = match lock.local_notify.clone() {
        //     Some(n) => n,
        //     None => return,
        // };
        // drop(lock);

        // let executed_file = FileDescription {
        //     path: local_notify.notify_file_path.clone(),
        //     size: 0,
        //     total_lines: 0,
        // };
        // let file_position = FilePosition::default();

        // let mut offset = match TryInto::<u64>::try_into(file_position.offset) {
        //     Ok(o) => o,
        //     Err(e) => {
        //         log::error!("{:?}", e);
        //         return;
        //     }
        // };
        // let mut line_num = file_position.line_num;

        // let subffix = offset.to_string();
        // let mut offset_key = OFFSET_PREFIX.to_string();
        // offset_key.push_str(&subffix);

        // // 启动 checkpoint 记录器
        // let task_status_saver = TaskStatusSaver {
        //     check_point_path: assistant.lock().await.check_point_path.clone(),
        //     executed_file,
        //     stop_mark: Arc::clone(&stop_mark),
        //     list_file_positon_map: Arc::clone(&offset_map),
        //     file_for_notify: Some(local_notify.notify_file_path.clone()),
        //     task_stage: TaskStage::Increment,
        //     interval: 3,
        //     object_list_files: None,
        // };

        // let task_id = self.task_id.clone();
        // task::spawn(async move {
        //     task_status_saver.snapshot_to_file(task_id).await;
        // });

        // let error_file_name = gen_file_path(
        //     &self.attributes.meta_dir,
        //     TRANSFER_ERROR_RECORD_PREFIX,
        //     &subffix,
        // );

        // let regex_filter =
        //     match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
        //         Ok(r) => r,
        //         Err(e) => {
        //             log::error!("{:?}", e);
        //             return;
        //         }
        //     };
        // let error_file = match OpenOptions::new()
        //     .create(true)
        //     .write(true)
        //     .truncate(true)
        //     .open(error_file_name.as_str())
        // {
        //     Ok(f) => f,
        //     Err(e) => {
        //         log::error!("{:?}", e);
        //         return;
        //     }
        // };

        // drop(error_file);

        // loop {
        //     if local_notify
        //         .notify_file_size
        //         .load(Ordering::SeqCst)
        //         .le(&offset)
        //     {
        //         tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        //         continue;
        //     }

        //     let mut file = match File::open(&local_notify.notify_file_path) {
        //         Ok(f) => f,
        //         Err(e) => {
        //             log::error!("{:?}", e);
        //             stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
        //             return;
        //         }
        //     };

        //     if let Err(e) = file.seek(SeekFrom::Start(offset)) {
        //         log::error!("{:?}", e);
        //         stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
        //         continue;
        //     };

        //     let lines = BufReader::new(file).lines();
        //     let mut offset_usize: usize = TryInto::<usize>::try_into(offset).unwrap();
        //     let mut records = vec![];
        //     for line in lines {
        //         line_num += 1;
        //         if let Result::Ok(key) = line {
        //             // Modifed 解析
        //             offset_usize += key.len();

        //             match self
        //                 .modified_str_to_record_description(
        //                     &key,
        //                     &local_notify.notify_file_path,
        //                     offset_usize,
        //                     line_num,
        //                 )
        //                 .await
        //             {
        //                 Ok(r) => {
        //                     if regex_filter.passed(&r.source_key) {
        //                         records.push(r);
        //                     }
        //                 }
        //                 Err(e) => {
        //                     let r = RecordOption {
        //                         source_key: "".to_string(),
        //                         target_key: "".to_string(),
        //                         list_file_path: local_notify.notify_file_path.clone(),
        //                         list_file_position: FilePosition {
        //                             file_num: -1,
        //                             offset: offset_usize,
        //                             line_num,
        //                         },
        //                         option: Opt::UNKOWN,
        //                     };
        //                     r.handle_error(stop_mark.clone(), err_occur.clone(), &error_file_name);
        //                     log::error!("{:?}", e);
        //                 }
        //             }
        //         }
        //     }

        //     if records.len() > 0 {
        //         let copy = TransferLocal2LocalExecutor {
        //             source: self.source.clone(),
        //             target: self.target.clone(),
        //             stop_mark: stop_mark.clone(),
        //             err_occur: Arc::new(AtomicBool::new(false)),
        //             offset_map: Arc::clone(&offset_map),
        //             attributes: self.attributes.clone(),
        //             list_file_path: local_notify.notify_file_path.clone(),
        //         };
        //         let _ = copy.transfer_record_options(records).await;
        //     }

        //     let error_file = match File::open(&error_file_name) {
        //         Ok(f) => f,
        //         Err(e) => {
        //             log::error!("{:?}", e);
        //             err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
        //             return;
        //         }
        //     };

        //     match error_file.metadata() {
        //         Ok(meta) => {
        //             if meta.len() == 0 {
        //                 let _ = fs::remove_file(error_file_name.as_str());
        //             }
        //         }
        //         Err(_) => {}
        //     };
        //     offset = local_notify.notify_file_size.load(Ordering::SeqCst);
        //     let offset_usize = TryInto::<usize>::try_into(offset).unwrap();
        //     let position = FilePosition {
        //         file_num: -1,
        //         offset: offset_usize,
        //         line_num,
        //     };

        //     offset_map.remove(&offset_key);
        //     offset_key = OFFSET_PREFIX.to_string();
        //     offset_key.push_str(&offset.to_string());
        //     offset_map.insert(offset_key.clone(), position);
        // }
    }
}

impl TransferLocal2Local {
    async fn increment_notify(
        &self,
        _joinset: &mut JoinSet<()>,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        _semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        checkpoint_path: &str,
        file_for_notify: String,
        // assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()> {
        // let lock = assistant.lock().await;
        // let local_notify = match lock.local_notify.clone() {
        //     Some(n) => n,
        //     None => return,
        // };
        // drop(lock);

        // let executed_file = FileDescription {
        //     path: local_notify.notify_file_path.clone(),
        //     size: 0,
        //     total_lines: 0,
        // };

        let executed_file = FileDescription {
            path: file_for_notify.clone(),
            size: 0,
            total_lines: 0,
        };
        let file_position = FilePosition::default();

        // let mut offset = match TryInto::<u64>::try_into(file_position.offset) {
        //     Ok(o) => o,
        //     Err(e) => {
        //         log::error!("{:?}", e);
        //         return;
        //     }
        // };

        let mut offset = TryInto::<u64>::try_into(file_position.offset).context(format!(
            "{}:{}",
            file!(),
            line!()
        ))?;
        let mut line_num = file_position.line_num;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        // 启动 checkpoint 记录器
        let task_status_saver = TaskStatusSaver {
            // check_point_path: assistant.lock().await.check_point_path.clone(),
            check_point_path: checkpoint_path.to_string(),
            executed_file,
            stop_mark: Arc::clone(&stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            file_for_notify: Some(file_for_notify.clone()),
            task_stage: TaskStage::Increment,
            interval: 3,
            object_list_files: None,
        };

        let task_id = self.task_id.clone();
        task::spawn(async move {
            task_status_saver.snapshot_to_file(task_id).await;
        });

        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );

        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)
                .context(format!("{}:{}", file!(), line!()))?;

        let error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())
            .context(format!("{}:{}", file!(), line!()))?;

        drop(error_file);

        while !stop_mark.load(Ordering::SeqCst) {
            let notify_file_size =
                count_file_bytes(&file_for_notify).context(format!("{}:{}", file!(), line!()))?;
            if notify_file_size.le(&offset) {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            let mut file =
                File::open(&file_for_notify).context(format!("{}:{}", file!(), line!()))?;

            if let Err(e) = file.seek(SeekFrom::Start(offset)) {
                log::error!("{:?}", e);
                stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                continue;
            };

            let lines = BufReader::new(file).lines();
            let mut offset_usize: usize =
                TryInto::<usize>::try_into(offset).context(format!("{}:{}", file!(), line!()))?;
            let mut records = vec![];
            for line in lines {
                line_num += 1;
                if let Result::Ok(key) = line {
                    // Modifed 解析
                    offset_usize += key.len();

                    match self
                        .modified_str_to_record_description(
                            &key,
                            // &local_notify.notify_file_path,
                            &file_for_notify,
                            offset_usize,
                            line_num,
                        )
                        .await
                    {
                        Ok(r) => {
                            if regex_filter.passed(&r.source_key) {
                                records.push(r);
                            }
                        }
                        Err(e) => {
                            let r = RecordOption {
                                source_key: "".to_string(),
                                target_key: "".to_string(),
                                // list_file_path: local_notify.notify_file_path.clone(),
                                list_file_path: file_for_notify.clone(),
                                list_file_position: FilePosition {
                                    file_num: -1,
                                    offset: offset_usize,
                                    line_num,
                                },
                                option: Opt::UNKOWN,
                            };
                            r.handle_error(stop_mark.clone(), err_occur.clone(), &error_file_name);
                            log::error!("{:?}", e);
                        }
                    }
                }
            }

            if records.len() > 0 {
                let copy = TransferLocal2LocalExecutor {
                    source: self.source.clone(),
                    target: self.target.clone(),
                    stop_mark: stop_mark.clone(),
                    err_occur: Arc::new(AtomicBool::new(false)),
                    offset_map: Arc::clone(&offset_map),
                    attributes: self.attributes.clone(),
                    // list_file_path: local_notify.notify_file_path.clone(),
                    list_file_path: file_for_notify.clone(),
                };
                let _ = copy.transfer_record_options(records).await;
            }

            let error_file =
                File::open(&error_file_name).context(format!("{}:{}", file!(), line!()))?;

            match error_file.metadata() {
                Ok(meta) => {
                    if meta.len() == 0 {
                        let _ = fs::remove_file(error_file_name.as_str());
                    }
                }
                Err(_) => {}
            };
            // offset = local_notify.notify_file_size.load(Ordering::SeqCst);
            offset =
                count_file_bytes(&file_for_notify).context(format!("{}:{}", file!(), line!()))?;
            let offset_usize = TryInto::<usize>::try_into(offset).unwrap();
            let position = FilePosition {
                file_num: -1,
                offset: offset_usize,
                line_num,
            };

            offset_map.remove(&offset_key);
            offset_key = OFFSET_PREFIX.to_string();
            offset_key.push_str(&offset.to_string());
            offset_map.insert(offset_key.clone(), position);
        }
        Ok(())
    }

    async fn increment_scan(
        &self,
        execute_set: &mut JoinSet<()>,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        checkpoint_path: &str,
        // assistant: Arc<Mutex<IncrementAssistant>>,
        interval: u64,
    ) -> Result<()> {
        // 循环执行获取 lastmodify 大于 checkpoint 指定的时间戳的对象
        // let lock = assistant.lock().await;
        // let checkpoint_path = lock.check_point_path.clone();
        // let mut checkpoint = match get_task_checkpoint(&lock.check_point_path) {
        //     Ok(c) => c,
        //     Err(e) => {
        //         log::error!("{:?}", e);
        //         return;
        //     }
        // };
        // checkpoint.task_stage = TaskStage::Increment;
        // drop(lock);
        let mut checkpoint =
            get_task_checkpoint(checkpoint_path).context(format!("{}:{}", file!(), line!()))?;
        checkpoint.task_stage = TaskStage::Increment;

        let regex_filter =
            RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include)
                .context(format!("{}:{}", file!(), line!()))?;

        let pd = prompt_processbar("executing increment:waiting for data...");
        let mut finished_total_objects = 0;

        while !stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let modified = self
                .changed_object_capture_based_target(
                    usize::try_from(checkpoint.task_begin_timestamp).context(format!(
                        "{}:{}",
                        file!(),
                        line!()
                    ))?,
                )
                .await
                .context(format!("{}:{}", file!(), line!()))?;

            let mut vec_keys = vec![];
            // 生成执行文件
            let mut list_file_position = FilePosition::default();
            // let modified_file = match File::open(&modified.path) {
            //     Ok(f) => f,
            //     Err(e) => {
            //         log::error!("{:?}", e);
            //         err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
            //         stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
            //         return;
            //     }
            // };
            let modified_file =
                File::open(&modified.path).context(format!("{}:{}", file!(), line!()))?;

            // 按列表传输 object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(modified_file).lines();
            for (idx, line) in lines.enumerate() {
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();

                    // let record = match from_str::<RecordOption>(&line_str) {
                    //     Ok(r) => r,
                    //     Err(e) => {
                    //         log::error!("{:?}", e);
                    //         err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                    //         stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    //         return;
                    //     }
                    // };
                    let record = from_str::<RecordOption>(&line_str).context(format!(
                        "{}:{}",
                        file!(),
                        line!()
                    ))?;
                    list_file_position.offset += len;
                    list_file_position.line_num += 1;

                    if !regex_filter.passed(&record.source_key) {
                        continue;
                    }
                    vec_keys.push(record);
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_transfer_batch.to_string())
                    || idx.eq(&TryInto::<usize>::try_into(modified.total_lines - 1)?)
                {
                    while execute_set.len() >= self.attributes.task_parallelism {
                        execute_set.join_next().await;
                    }
                    let vk: Vec<RecordOption> = vec_keys.clone();
                    let executor = self.gen_transfer_executor(
                        stop_mark.clone(),
                        err_occur.clone(),
                        semaphore.clone(),
                        offset_map.clone(),
                        modified.path.clone(),
                    );

                    execute_set.spawn(async move {
                        let _ = executor.transfer_record_options(vk).await;
                    });

                    // 清理临时 key vec
                    vec_keys.clear();
                }
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            finished_total_objects += modified.total_lines;
            if !modified.total_lines.eq(&0) {
                let msg: String = format!(
                    "executing transfer modified finished this batch {} total {};",
                    modified.total_lines, finished_total_objects
                );
                let log_info = LogInfo::<String> {
                    task_id: self.task_id.clone(),
                    msg,
                    additional: None,
                };

                log::info!("{:?}", log_info);
            }

            let _ = fs::remove_file(&modified.path);
            checkpoint.executing_file_position = FilePosition {
                // Todo 详细了解该函数功能后调整 file_num,
                file_num: -1,
                offset: modified.size.try_into().unwrap(),
                line_num: modified.total_lines,
            };
            checkpoint.executing_file = modified.clone();
            checkpoint.task_begin_timestamp = i128::from(now.as_secs());

            let _ = checkpoint.save_to(&checkpoint_path);
            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        }
        pd.finish();
        Ok(())
    }

    async fn capture_removed_objects_to_file(&self, to_file: &str) -> Result<()> {
        let mut removed_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&to_file)?;

        for entry in WalkDir::new(&self.target)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.target) {
                    continue;
                }

                let key = match &self.target.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                let source_key_str = gen_file_path(&self.source, key, "");
                let source_path = Path::new(&source_key_str);
                if !source_path.exists() {
                    let record = RecordOption {
                        source_key: source_key_str,
                        target_key: p.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::REMOVE,
                    };
                    let record_str = struct_to_json_string(&record)?;
                    removed_file.write_all(record_str.as_bytes())?;
                    removed_file.write_all("\n".as_bytes())?;
                }
            };
        }
        removed_file.flush()?;
        Ok(())
    }

    async fn capture_modified_objects_to_file(
        &self,
        last_capture_timestamp: usize,
        to_file: &str,
    ) -> Result<()> {
        let mut modified_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&to_file)?;

        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        let last_modify_filter = LastModifyFilter {
            filter_type: LastModifyFilterType::Greater,
            timestamp: last_capture_timestamp,
        };
        for entry in WalkDir::new(&self.source)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.source) {
                    continue;
                }
                if let Some(f) = &regex_filter {
                    if !f.passed(p) {
                        continue;
                    }
                }

                let key = match &self.source.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                let target_key_str = gen_file_path(&self.target, key, "");

                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();

                if last_modify_filter.passed(usize::try_from(modified_time)?) {
                    let record = RecordOption {
                        source_key: p.to_string(),
                        target_key: target_key_str,
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::PUT,
                    };
                    let record_str = struct_to_json_string(&record)?;
                    modified_file.write_all(record_str.as_bytes())?;
                    modified_file.write_all("\n".as_bytes())?;
                }
            };
        }
        modified_file.flush()?;
        Ok(())
    }

    async fn modified_str_to_record_description(
        &self,
        modified_str: &str,
        list_file_path: &str,
        offset: usize,
        line_num: u64,
    ) -> Result<RecordOption> {
        let modified = from_str::<Modified>(modified_str)?;
        let mut target_path = modified.path.clone();

        match self.source.ends_with("/") {
            true => target_path.drain(..self.source.len()),
            false => target_path.drain(..self.source.len() + 1),
        };

        match self.target.ends_with("/") {
            true => {
                target_path.insert_str(0, &self.target);
            }
            false => {
                let target_prefix = self.target.clone() + "/";
                target_path.insert_str(0, &target_prefix);
            }
        }

        if PathType::File.eq(&modified.path_type) {
            match modified.modify_type {
                ModifyType::Create | ModifyType::Modify => {
                    let record = RecordOption {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        // Todo 理解该函数的使用场景后重新对 file_num 取相关值
                        list_file_position: FilePosition {
                            file_num: -1,
                            offset,
                            line_num,
                        },
                        option: Opt::PUT,
                    };
                    return Ok(record);
                }
                ModifyType::Delete => {
                    let record = RecordOption {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        // Todo 理解该函数的使用场景后重新对 file_num 取相关值
                        list_file_position: FilePosition {
                            file_num: -1,
                            offset,
                            line_num,
                        },
                        option: Opt::REMOVE,
                    };
                    return Ok(record);
                }
                ModifyType::Unkown => Err(anyhow!("Unkown modify type")),
            }
        } else {
            return Err(anyhow!("Unkown modify type"));
        }
    }
}

#[derive(Debug, Clone)]
struct TransferLocal2LocalExecutor {
    pub source: String,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferLocal2LocalExecutor {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let mut offset_key = OFFSET_PREFIX.to_string();
        let subffix = records[0].file_num.to_string() + "_" + &records[0].offset.to_string();
        offset_key.push_str(&subffix);

        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );
        create_parent_dir(&error_file_name)?;

        let error_file = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())
        {
            Ok(f) => f,
            Err(e) => {
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.stop_mark
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(anyhow!("{}", e));
            }
        };

        drop(error_file);

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            let s_file_name = gen_file_path(self.source.as_str(), record.key.as_str(), "");
            let t_file_name = gen_file_path(self.target.as_str(), record.key.as_str(), "");

            match self
                .listed_record_handler(s_file_name.as_str(), t_file_name.as_str())
                .await
            {
                Ok(_) => {
                    // 文件位置记录后置，避免中断时已记录而传输未完成，续传时丢记录
                    self.offset_map.insert(
                        offset_key.clone(),
                        FilePosition {
                            file_num: record.file_num,
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                    );
                }
                Err(e) => {
                    // 记录错误记录
                    let opt = RecordOption {
                        source_key: s_file_name,
                        target_key: t_file_name,
                        list_file_path: self.list_file_path.clone(),
                        list_file_position: FilePosition {
                            file_num: record.file_num,
                            offset: record.offset,
                            line_num: record.line_num,
                        },
                        option: Opt::PUT,
                    };
                    opt.handle_error(
                        self.stop_mark.clone(),
                        self.err_occur.clone(),
                        &error_file_name,
                    );
                    log::error!("{:?},{:?}", e, opt);
                }
            };
        }

        self.offset_map.remove(&offset_key);

        let error_file = match File::open(&error_file_name) {
            Ok(f) => f,
            Err(e) => {
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);
                return Err(anyhow!(e));
            }
        };

        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };

        Ok(())
    }

    async fn transfer_record_options(&self, records: Vec<RecordOption>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let mut subffix = records[0].list_file_position.offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        subffix.push_str("_");
        subffix.push_str(now.as_secs().to_string().as_str());

        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );

        let error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        drop(error_file);

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            if let Err(e) = self.record_description_handler(&record).await {
                record.handle_error(
                    self.stop_mark.clone(),
                    self.err_occur.clone(),
                    &error_file_name,
                );
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);
            }
        }

        let error_file = match File::open(&error_file_name) {
            Ok(f) => f,
            Err(e) => {
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);
                return Err(anyhow!(e));
            }
        };
        match error_file.metadata() {
            Ok(meta) => {
                if meta.len() == 0 {
                    let _ = fs::remove_file(error_file_name.as_str());
                }
            }
            Err(_) => {}
        };

        Ok(())
    }
}

impl TransferLocal2LocalExecutor {
    async fn listed_record_handler(&self, source_file: &str, target_file: &str) -> Result<()> {
        // 判断源文件是否存在，若不存判定为成功传输
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            return Ok(());
        }

        let t_path = Path::new(target_file);
        // if let Some(p) = t_path.parent() {
        //     std::fs::create_dir_all(p)?
        // };

        // 目标 object 存在则不推送
        if self.attributes.target_exists_skip {
            if t_path.exists() {
                return Ok(());
            }
        }

        copy_file(
            source_file,
            target_file,
            self.attributes.large_file_size,
            self.attributes.multi_part_chunk_size,
        )
    }

    pub async fn record_description_handler(&self, record: &RecordOption) -> Result<()> {
        match record.option {
            Opt::PUT => {
                copy_file(
                    &record.source_key,
                    &record.target_key,
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )?;
            }
            Opt::REMOVE => fs::remove_file(record.target_key.as_str())?,
            _ => return Err(anyhow!("unknow option")),
        };
        Ok(())
    }
}
