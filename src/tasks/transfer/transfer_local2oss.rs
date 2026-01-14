// use super::task_transfer::{IncrementMode, TransferTaskAttributes};
use crate::checkpoint::{get_task_checkpoint, ListedRecord, Opt, RecordOption};
use crate::commons::{
    analyze_folder_files_size, count_file_bytes, create_parent_dir, json_to_struct, merge_file,
    prompt_processbar, read_lines, scan_folder_files_to_multi_files, struct_to_json_string,
    Modified, ModifyType, NotifyWatcher, PathType, RegexFilter,
};
use crate::consts::task_consts::{
    MODIFIED_PREFIX, OFFSET_PREFIX, REMOVED_PREFIX, TRANSFER_ERROR_RECORD_PREFIX,
};
use crate::models::model_checkpoint::{FileDescription, FilePosition};
use crate::models::model_filters::{LastModifyFilter, LastModifyFilterType};
use crate::models::model_s3::OSSDescription;
use crate::models::model_task::TaskStage;
use crate::models::model_task_default_parameters::TaskDefaultParameters;
use crate::models::model_task_transfer::{IncrementMode, TransferTaskAttributes};
use crate::s3::oss_client::OssClient;
use crate::tasks::LogInfo;
use crate::tasks::{
    gen_file_path,
    task_traits::{TransferExecutor, TransferTaskActions},
    TaskStatusSaver,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::Object;
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
use tokio::{
    sync::Semaphore,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
/// 表示从本地文件系统到 OSS 的传输任务
pub struct TransferLocal2Oss {
    /// 任务的唯一标识符，默认值由 TaskDefaultParameters::id_default 方法提供
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    /// 任务的名称，默认值由 TaskDefaultParameters::name_default 方法提供
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    /// 本地文件系统中的源路径
    pub source: String,
    /// 目标 OSS 的描述信息
    pub target: OSSDescription,
    /// 传输任务的属性
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferLocal2Oss {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            target: OSSDescription::default(),
            source: "/tmp".to_string(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferLocal2Oss {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        analyze_folder_files_size(
            &self.source,
            regex_filter,
            self.attributes.last_modify_filter.clone(),
        )
    }
    // 错误记录重试
    async fn error_record_retry(
        &self,
        stop_mark: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        // 遍历错误记录
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
                        let executor = self.gen_transfer_executor(
                            stop_mark.clone(),
                            Arc::new(AtomicBool::new(false)),
                            semaphore.clone(),
                            Arc::new(DashMap::<String, FilePosition>::new()),
                            p.to_string(),
                        );

                        executor.transfer_record_options(record_vec).await?;
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
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file_path: String,
    ) -> Arc<dyn TransferExecutor + Send + Sync> {
        let executor = TransferLocal2OssExecuter {
            source: self.source.clone(),
            target: self.target.clone(),
            stop_mark,
            err_occur,
            semaphore,
            offset_map,
            attributes: self.attributes.clone(),
            list_file_path,
        };
        Arc::new(executor)
    }

    // 生成对象列表
    async fn list_objects_to_multi_files(&self, meta_dir: &str) -> Result<Vec<FileDescription>> {
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)
                .context(format!("{}:{}", file!(), line!()))?;
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
        file_for_notify: Option<String>,
        // assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()> {
        let notify_file_path = match file_for_notify {
            Some(f) => f,
            None => {
                return Err(anyhow!("notify file is none"));
            }
        };

        let file_for_notify = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(notify_file_path.as_str())?;

        let watcher = NotifyWatcher::new(&self.source)?;
        let notify_file_size = Arc::new(AtomicU64::new(0));

        watcher
            .watch_to_file(
                stop_mark,
                err_occur,
                file_for_notify,
                Arc::clone(&notify_file_size),
            )
            .await;
        log::info!("notify");

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
    }
}

impl TransferLocal2Oss {
    async fn increment_notify(
        &self,
        _joinset: &mut JoinSet<()>,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        checkpoint_path: &str,
        file_for_notify: String,
        // assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()> {
        // let lock = assistant.lock().await;
        // let file_for_notify = match lock.local_notify.clone() {
        //     Some(n) => n,
        //     None => {
        //         return;
        //     }
        // };
        // drop(lock);

        let mut offset = 0;
        let mut line_num = 0;

        let subffix = offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let executed_file = FileDescription {
            // path: file_for_notify.notify_file_path.clone(),
            path: file_for_notify.clone(),
            size: 0,
            total_lines: 0,
        };

        let task_status_saver = TaskStatusSaver {
            check_point_path: checkpoint_path.to_string(),
            executed_file,
            stop_mark: Arc::clone(&stop_mark),
            list_file_positon_map: Arc::clone(&offset_map),
            // file_for_notify: Some(file_for_notify.notify_file_path.clone()),
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

        let pd = prompt_processbar("executing increment:waiting for data...");

        while !stop_mark.load(Ordering::SeqCst) {
            // if file_for_notify
            //     .notify_file_size
            //     .load(Ordering::SeqCst)
            //     .le(&offset)
            // {
            //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            //     continue;
            // }
            let notify_file_size = count_file_bytes(&file_for_notify)?;
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

            let error_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())
                .unwrap();

            drop(error_file);

            // ToDo 考虑到文件一直写入情况，如何准确的定位到现有文件的尾部，可一实施复制方案，复制到另一个文件，然后删除
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
                                list_file_path: file_for_notify.clone(),
                                list_file_position: FilePosition {
                                    // Todo 对代码了解后调整 file_num 的值
                                    file_num: -1,
                                    offset: offset_usize,
                                    line_num,
                                },
                                option: Opt::UNKOWN,
                            };
                            r.handle_error(stop_mark.clone(), err_occur.clone(), &error_file_name);
                            err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                            log::error!("{:?}", e);
                        }
                    }
                }
            }

            let executor = self.gen_transfer_executor(
                stop_mark.clone(),
                Arc::new(AtomicBool::new(false)),
                semaphore.clone(),
                offset_map.clone(),
                file_for_notify.clone(),
            );

            if records.len() > 0 {
                let _ = executor.transfer_record_options(records).await;
            }

            // let error_file = match File::open(&error_file_name) {
            //     Ok(f) => f,
            //     Err(e) => {
            //         log::error!("{:?}", e);
            //         err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
            //         stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
            //         return;
            //     }
            // };

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

            offset = count_file_bytes(&file_for_notify)?;
            let offset_usize = TryInto::<usize>::try_into(offset).unwrap();
            let position = FilePosition {
                //Todo 待了解业务逻辑后调整 file_num
                file_num: -1,
                offset: offset_usize,
                line_num,
            };

            offset_map.remove(&offset_key);
            offset_key = OFFSET_PREFIX.to_string();
            offset_key.push_str(&offset.to_string());
            offset_map.insert(offset_key.clone(), position);
        }
        pd.finish();
        Ok(())
    }

    // 上层函数追踪到错误实现错误处理将 stop_mark、err_occur 转为 true
    async fn increment_scan(
        &self,
        execute_set: &mut JoinSet<()>,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        checkpoint_path: &str,
        interval: u64,
    ) -> Result<()> {
        // 循环执行获取 lastmodify 大于 checkpoint 指定的时间戳的对象
        let mut checkpoint = match get_task_checkpoint(checkpoint_path) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(e);
            }
        };
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
            let modified_file =
                File::open(&modified.path).context(format!("{}:{}", file!(), line!()))?;

            // 按列表传输 object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(modified_file).lines();
            for (idx, line) in lines.enumerate() {
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();

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

                // 批量传输条件：临时队列达到批量值，执行到每一文件的最后一行
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
                offset: modified
                    .size
                    .try_into()
                    .context(format!("{}:{}", file!(), line!()))?,
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
        create_parent_dir(to_file).context(format!("{}:{}", file!(), line!()))?;
        let mut removed_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(to_file)?;

        let mut persist_removed_objects = |objects: Vec<Object>| -> Result<()> {
            for obj in objects {
                if let Some(target_key) = obj.key() {
                    let mut source_key = "";
                    if let Some(p) = &self.target.prefix {
                        source_key = match p.ends_with("/") {
                            true => &p[p.len()..],
                            false => &p[p.len() + 1..],
                        };
                    };
                    let source_key_str = gen_file_path(&self.source, source_key, "");
                    let source_path = Path::new(&source_key_str);
                    if !source_path.exists() {
                        let record = RecordOption {
                            source_key: source_key_str,
                            target_key: target_key.to_string(),
                            list_file_path: "".to_string(),
                            list_file_position: FilePosition::default(),
                            option: Opt::REMOVE,
                        };
                        let record_str = struct_to_json_string(&record)?;
                        removed_file.write_all(record_str.as_bytes())?;
                        removed_file.write_all("\n".as_bytes())?;
                    }
                }
            }
            Ok(())
        };

        let target_client = self.target.gen_oss_client()?;
        let resp = target_client
            .list_objects(
                &self.target.bucket,
                self.target.prefix.clone(),
                self.attributes.objects_transfer_batch,
                None,
            )
            .await?;
        let mut token = resp.next_token;

        if let Some(objects) = resp.object_list {
            persist_removed_objects(objects)?;
        }

        while token.is_some() {
            let resp = target_client
                .list_objects(
                    &self.target.bucket,
                    self.target.prefix.clone(),
                    self.attributes.objects_transfer_batch,
                    token,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                persist_removed_objects(objects)?;
            }
            token = resp.next_token;
        }

        removed_file.flush()?;

        Ok(())
    }

    async fn capture_modified_objects_to_file(
        &self,
        last_capture_timestamp: usize,
        to_file: &str,
    ) -> Result<()> {
        create_parent_dir(to_file).context(format!("{}:{}", file!(), line!()))?;
        let mut modified_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(to_file)?;
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
                    true => &p[self.source.len()..],
                    false => &p[self.source.len() + 1..],
                };

                let mut target_key = "".to_string();
                if let Some(p) = &self.target.prefix {
                    target_key.push_str(p);
                }
                target_key.push_str(key);

                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();

                if last_modify_filter.passed(usize::try_from(modified_time)?) {
                    let record = RecordOption {
                        source_key: p.to_string(),
                        target_key: target_key.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::PUT,
                    };
                    let record_str = match struct_to_json_string(&record) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{:?}", e);
                            return Err(e);
                        }
                    };
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

        // 截取 target key 相对路径
        match self.source.ends_with("/") {
            true => target_path.drain(..self.source.len()),
            false => target_path.drain(..self.source.len() + 1),
        };

        // 补全 prefix
        if let Some(mut oss_path) = self.target.prefix.clone() {
            match oss_path.ends_with("/") {
                true => target_path.insert_str(0, &oss_path),
                false => {
                    oss_path.push('/');
                    target_path.insert_str(0, &oss_path);
                }
            }
        }

        if PathType::File.eq(&modified.path_type) {
            match modified.modify_type {
                ModifyType::Create | ModifyType::Modify => {
                    let record = RecordOption {
                        source_key: modified.path.clone(),
                        target_key: target_path,
                        list_file_path: list_file_path.to_string(),
                        list_file_position: FilePosition {
                            //Todo 待了解业务逻辑后调整 file_num
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
                        list_file_position: FilePosition {
                            //Todo 待了解业务逻辑后调整 file_num
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
/// 表示从本地文件系统到 OSS 的传输任务执行器
struct TransferLocal2OssExecuter {
    /// 本地文件系统中的源路径
    pub source: String,
    /// 目标 OSS 的描述信息
    pub target: OSSDescription,
    /// 用于标记任务是否已停止的原子布尔值
    pub stop_mark: Arc<AtomicBool>,
    /// 用于标记任务执行过程中是否发生错误的原子布尔值
    pub err_occur: Arc<AtomicBool>,
    /// 用于控制并发的信号量
    pub semaphore: Arc<Semaphore>,
    /// 用于存储文件位置信息的哈希映射
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    /// 传输任务的属性
    pub attributes: TransferTaskAttributes,
    /// 记录列表文件的路径
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferLocal2OssExecuter {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        // ToDo 待修改其他模块，统一 subffix 的取值规则
        let mut offset_key = OFFSET_PREFIX.to_string();
        let subffix = records[0].file_num.to_string() + "_" + &records[0].offset.to_string();
        offset_key.push_str(&subffix);

        // Todo
        // 若第一行出错则整组 record 写入错误记录，若错误记录文件打开报错则停止任务
        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );

        create_parent_dir(&error_file_name)?;
        {
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())?;
        }

        let target_oss_client =
            match self
                .target
                .gen_oss_client()
                .context(format!("{}:{}", file!(), line!()))
            {
                Ok(c) => c,
                Err(e) => {
                    self.err_occur
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    self.stop_mark
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    log::error!("{:?}", e);
                    return Err(anyhow!(e));
                }
            };

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            let source_file_path = gen_file_path(self.source.as_str(), &record.key.as_str(), "");
            let mut target_key = "".to_string();
            if let Some(s) = self.target.prefix.clone() {
                target_key.push_str(&s);
            };

            target_key.push_str(&record.key);

            if let Err(e) = self
                .listed_record_handler(&source_file_path, &target_oss_client, &target_key)
                .await
            {
                let record_option = RecordOption {
                    source_key: source_file_path.clone(),
                    target_key: target_key.clone(),
                    list_file_path: self.list_file_path.clone(),
                    list_file_position: FilePosition {
                        file_num: record.file_num,
                        offset: record.offset,
                        line_num: record.line_num,
                    },
                    option: Opt::PUT,
                };
                record_option.handle_error(
                    self.stop_mark.clone(),
                    self.err_occur.clone(),
                    &error_file_name,
                    // offset_key.as_str(),
                );
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.stop_mark
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?} {:?}", e, record_option);
            }

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
                if 0.eq(&meta.len()) {
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

        {
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())?;
        }

        let c_t = self.target.gen_oss_client()?;

        // Todo
        // 增加去重逻辑，当两条记录相邻为 create 和 modif 时只 put 一次
        // 增加目录删除逻辑，对应 oss 删除指定 prefix 下的所有文件，文件系统删除目录
        for record in records {
            // 记录执行文件位置
            self.offset_map
                .insert(offset_key.clone(), record.list_file_position.clone());

            // 目标 object 存在则不推送
            if self.attributes.target_exists_skip {
                match c_t
                    .object_exists(self.target.bucket.as_str(), &record.target_key)
                    .await
                {
                    Ok(b) => {
                        if b {
                            continue;
                        }
                    }
                    Err(e) => {
                        record.handle_error(
                            self.stop_mark.clone(),
                            self.err_occur.clone(),
                            &error_file_name,
                        );
                        log::error!("{:?}", e);
                        continue;
                    }
                }
            }

            if let Err(e) = match record.option {
                Opt::PUT => {
                    // 判断源文件是否存在
                    let s_path = Path::new(&record.source_key);
                    if !s_path.exists() {
                        continue;
                    }

                    c_t.upload_local_file(
                        self.target.bucket.as_str(),
                        &record.target_key,
                        &record.source_key,
                        self.attributes.large_file_size,
                        self.attributes.multi_part_chunk_size,
                    )
                    .await
                }
                Opt::REMOVE => {
                    match c_t
                        .remove_object(self.target.bucket.as_str(), &record.target_key)
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(e) => Err(anyhow!("{:?}", e)),
                    }
                }
                _ => Err(anyhow!("option unkown")),
            } {
                record.handle_error(
                    self.stop_mark.clone(),
                    self.err_occur.clone(),
                    &error_file_name,
                );
                log::error!("{:?}", e);
                continue;
            }
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
}

impl TransferLocal2OssExecuter {
    async fn listed_record_handler(
        &self,
        source_file: &str,
        target_oss: &OssClient,
        target_key: &str,
    ) -> Result<()> {
        // 判断源文件是否存在
        let s_path = Path::new(source_file);
        if !s_path.exists() {
            return Ok(());
        }

        // 目标 object 存在则不推送
        if self.attributes.target_exists_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await?;

            if target_obj_exists {
                return Ok(());
            }
        }

        // ToDo
        // 新增阐述 chunk_batch 定义分片上传每批上传分片的数量
        target_oss
            .upload_local_file_paralle(
                source_file,
                self.target.bucket.as_str(),
                target_key,
                self.attributes.large_file_size,
                self.semaphore.clone(),
                self.attributes.multi_part_chunk_size,
                self.attributes.multi_part_chunks_per_batch,
                self.attributes.multi_part_parallelism,
            )
            .await
    }
}
