use crate::models::model_task_default_parameters::TaskDefaultParameters;
use crate::{
    checkpoint::{get_task_checkpoint, ListedRecord, Opt, RecordOption},
    commons::{
        json_to_struct, merge_file, prompt_processbar, read_lines, struct_to_json_string,
        RegexFilter,
    },
    consts::task_consts::{
        MODIFIED_PREFIX, OFFSET_PREFIX, REMOVED_PREFIX, TRANSFER_ERROR_RECORD_PREFIX,
    },
    models::{
        model_checkpoint::{FileDescription, FilePosition},
        model_filters::{LastModifyFilter, LastModifyFilterType},
        model_s3::OSSDescription,
        model_task::TaskStage,
        model_task_transfer::{IncrementMode, TransferTaskAttributes},
    },
    s3::oss_client::{download_object, OssClient},
};
use crate::{
    commons::create_parent_dir,
    tasks::{
        gen_file_path,
        task_traits::{TransferExecutor, TransferTaskActions},
    },
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::Object;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Write},
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Semaphore, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferOss2Local {
    // 任务 ID，默认值为 TaskDefaultParameters::id_default()
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    // 任务名称，默认值为 TaskDefaultParameters::name_default()
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,

    pub source: OSSDescription,
    pub target: String,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferOss2Local {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: OSSDescription::default(),
            target: "/tmp".to_string(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Local {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let client = self.source.gen_oss_client()?;
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        client
            .analyze_objects_size(
                &self.source.bucket,
                self.source.prefix.clone(),
                regex_filter,
                self.attributes.last_modify_filter.clone(),
                self.attributes.objects_list_batch,
                self.attributes.task_parallelism,
            )
            .await
    }

    fn gen_transfer_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file_path: String,
    ) -> Arc<dyn TransferExecutor + Send + Sync> {
        let executor = TransferOss2LocalRecordsExecutor {
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

    async fn error_record_retry(
        &self,
        stop_mark: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        // 遍历错误记录
        // 每个错误文件重新处理
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

    async fn list_objects_to_multi_files(&self, meta_dir: &str) -> Result<Vec<FileDescription>> {
        let client_source = self.source.gen_oss_client()?;
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;
        client_source
            .append_object_list_to_multi_files(
                self.source.bucket.clone(),
                self.source.prefix.clone(),
                self.attributes.objects_list_batch,
                regex_filter,
                self.attributes.last_modify_filter,
                meta_dir,
                self.attributes.objects_list_file_max_line,
            )
            .await
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
        _stop_mark: Arc<AtomicBool>,
        _err_occur: Arc<AtomicBool>,
        _file_for_notify: Option<String>,
    ) -> Result<()> {
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
        _file_for_notify: Option<String>,
        increment_mode: IncrementMode,
    ) {
        let intervel = match increment_mode {
            IncrementMode::Scan { interval } => interval,
            _ => 3600,
        };
        // 循环执行获取 lastmodify 大于 checkpoint 指定的时间戳的对象
        let mut checkpoint = match get_task_checkpoint(checkpoint_path) {
            Ok(c) => c,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };
        checkpoint.task_stage = TaskStage::Increment;

        let regex_filter =
            match RegexFilter::from_vec(&self.attributes.exclude, &self.attributes.include) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

        // let mut sleep_time = 5;
        let pd = prompt_processbar("executing increment:waiting for data...");
        let mut finished_total_objects = 0;

        while !stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let modified = match self
                .changed_object_capture_based_target(
                    usize::try_from(checkpoint.last_scan_timestamp).unwrap(),
                )
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

            let mut vec_keys = vec![];
            // 生成执行文件
            let mut list_file_position = FilePosition::default();
            let modified_file = match File::open(&modified.path) {
                Ok(f) => f,
                Err(e) => {
                    log::error!("{:?}", e);
                    err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                    stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    return;
                }
            };

            // 按列表传输 object from source to target
            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(modified_file).lines();
            for line in lines {
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();
                    let mut record = match from_str::<RecordOption>(&line_str) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{:?}", e);
                            err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                            stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                            return;
                        }
                    };

                    if regex_filter.passed(&record.source_key) {
                        let t_file_name =
                            gen_file_path(self.target.as_str(), &record.target_key, "");
                        record.target_key = t_file_name;
                        vec_keys.push(record);
                    }
                    list_file_position.offset += len;
                    list_file_position.line_num += 1;
                };

                if vec_keys
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_transfer_batch.to_string())
                {
                    while execute_set.len() >= self.attributes.task_parallelism {
                        execute_set.join_next().await;
                    }
                    let vk = vec_keys.clone();
                    let executor = self.gen_transfer_executor(
                        stop_mark.clone(),
                        err_occur.clone(),
                        semaphore.clone(),
                        offset_map.clone(),
                        modified.path.clone(),
                    );
                    let _ = executor.transfer_record_options(vk).await;

                    // 清理临时 key vec
                    vec_keys.clear();
                }
            }

            if vec_keys.len() > 0 {
                while execute_set.len() >= self.attributes.task_parallelism {
                    execute_set.join_next().await;
                }

                let vk = vec_keys.clone();
                let executor = self.gen_transfer_executor(
                    stop_mark.clone(),
                    err_occur.clone(),
                    semaphore.clone(),
                    offset_map.clone(),
                    modified.path.clone(),
                );
                executor.transfer_record_options(vk).await;
            }

            while execute_set.len() > 0 {
                execute_set.join_next().await;
            }

            finished_total_objects += modified.total_lines;
            if !modified.total_lines.eq(&0) {
                let msg = format!(
                    "task {} executing transfer modified finished this batch {} total {};",
                    self.task_id, modified.total_lines, finished_total_objects
                );
                pd.set_message(msg);
            }

            let _ = fs::remove_file(&modified.path);

            checkpoint.executing_file_position = FilePosition {
                // Todo 详细了解该函数功能后调整 file_num,
                file_num: -1,
                offset: modified.size.try_into().unwrap(),
                line_num: modified.total_lines,
            };
            checkpoint.executing_file = modified.clone();
            checkpoint.last_scan_timestamp = i128::from(now.as_secs());
            let _ = checkpoint.save_to(&checkpoint_path);
            tokio::time::sleep(tokio::time::Duration::from_secs(intervel)).await;
        }
    }
}

impl TransferOss2Local {
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

        let source_client = self.source.gen_oss_client()?;

        // 筛选源对象，lastmodify 大于等于时间戳并转换为 RecordDescription 格式
        let mut persist_modified_objects = |objects: Vec<Object>| -> Result<()> {
            for obj in objects {
                if let Some(source_key) = obj.key() {
                    if let Some(d) = obj.last_modified() {
                        if let Some(ref f) = regex_filter {
                            if !f.passed(source_key) {
                                continue;
                            }
                        };
                        if last_modify_filter.passed(usize::try_from(d.secs())?) {
                            let target_key_str = gen_file_path(&self.target, source_key, "");
                            let record = RecordOption {
                                source_key: source_key.to_string(),
                                target_key: target_key_str,
                                list_file_path: "".to_string(),
                                list_file_position: FilePosition::default(),
                                option: Opt::PUT,
                            };

                            let record_str = struct_to_json_string(&record)?;
                            modified_file.write_all(record_str.as_bytes())?;
                            modified_file.write_all("\n".as_bytes())?;
                        }
                    }
                }
            }
            Ok(())
        };

        // 获取源所有增量 object 并写入文件
        let source_resp = source_client
            .list_objects(
                &self.source.bucket,
                self.source.prefix.clone(),
                self.attributes.objects_transfer_batch,
                None,
            )
            .await
            .context(format!("{}:{}", file!(), line!()))?;
        let mut source_token = source_resp.next_token;
        if let Some(objects) = source_resp.object_list {
            persist_modified_objects(objects)?;
        }

        while source_token.is_some() {
            let resp = source_client
                .list_objects(
                    &self.source.bucket,
                    self.source.prefix.clone(),
                    self.attributes.objects_transfer_batch,
                    source_token,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                persist_modified_objects(objects)?;
            }
            source_token = resp.next_token;
        }

        modified_file.flush()?;

        Ok(())
    }

    // 获取源端删除的对象
    async fn capture_removed_objects_to_file(&self, to_file: &str) -> Result<()> {
        let mut removed_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(to_file)?;
        let source_client = self.source.gen_oss_client()?;

        for entry in WalkDir::new(&self.target)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            if let Some(p) = entry.path().to_str() {
                if p.eq(&self.target) {
                    continue;
                }

                let source_key = match &self.target.ends_with("/") {
                    true => &p[self.target.len()..],
                    false => &p[self.target.len() + 1..],
                };

                if !source_client
                    .object_exists(&self.source.bucket, source_key)
                    .await?
                {
                    let record = RecordOption {
                        source_key: source_key.to_string(),
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
}

#[derive(Debug, Clone)]
struct TransferOss2LocalRecordsExecutor {
    pub source: OSSDescription,
    pub target: String,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    pub semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferOss2LocalRecordsExecutor {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        // ToDo 待修改其他模块，统一 subffix 的取值规则
        let mut offset_key = OFFSET_PREFIX.to_string();
        let subffix = records[0].file_num.to_string() + "_" + &records[0].offset.to_string();
        offset_key.push_str(&subffix);

        let error_file_name = gen_file_path(
            &self.attributes.meta_dir,
            TRANSFER_ERROR_RECORD_PREFIX,
            &subffix,
        );
        create_parent_dir(&error_file_name)?;

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let c_s = self.source.gen_oss_client()?;
        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }

            let t_file_name = gen_file_path(self.target.as_str(), &record.key.as_str(), "");

            if let Err(e) = self
                .listed_record_handler(&record, &c_s, t_file_name.as_str())
                .await
            {
                let record_option = RecordOption {
                    source_key: record.key.clone(),
                    target_key: t_file_name.clone(),
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
        let _ = error_file.flush();
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

        let mut error_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(error_file_name.as_str())?;

        let source_client = self.source.gen_oss_client()?;

        for record in records {
            let t_path = Path::new(&record.target_key);
            if let Some(p) = t_path.parent() {
                std::fs::create_dir_all(p)?
            };

            // 目标 object 存在则不推送
            if self.attributes.target_exists_skip {
                if t_path.exists() {
                    continue;
                }
            }

            match self
                .record_description_handler(&source_client, &record)
                .await
            {
                Ok(_) => {
                    // 记录执行文件位置
                    self.offset_map
                        .insert(offset_key.clone(), record.list_file_position.clone());
                }
                Err(e) => {
                    record.handle_error(
                        self.stop_mark.clone(),
                        self.err_occur.clone(),
                        &error_file_name,
                    );
                    log::error!("{:?},{:?}", e, record);
                }
            };
        }
        self.offset_map.remove(&offset_key);
        let _ = error_file.flush();
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

impl TransferOss2LocalRecordsExecutor {
    async fn listed_record_handler(
        &self,
        record: &ListedRecord,
        source_oss_client: &OssClient,
        target_file: &str,
    ) -> Result<()> {
        let t_path = Path::new(target_file);
        if let Some(p) = t_path.parent() {
            std::fs::create_dir_all(p)?;
        };

        // 目标 object 存在则不下载
        if self.attributes.target_exists_skip {
            if t_path.exists() {
                return Ok(());
            }
        }

        let s_obj_output = match source_oss_client
            .get_object(&self.source.bucket.as_str(), record.key.as_str())
            .await
        {
            core::result::Result::Ok(resp) => resp,
            Err(e) => {
                // 源端文件不存在按传输成功处理
                let service_err = e.into_service_error();
                match service_err.is_no_such_key() {
                    true => {
                        return Ok(());
                    }
                    false => {
                        return Err(service_err.into());
                    }
                }
            }
        };
        let content_len = match s_obj_output.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };
        let content_len_usize: usize = content_len.try_into()?;

        match content_len_usize.le(&self.attributes.large_file_size) {
            true => {
                download_object(
                    s_obj_output,
                    target_file,
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )
                .await
            }
            false => {
                source_oss_client
                    .download_object_by_range(
                        &self.source.bucket.clone(),
                        &record.key,
                        target_file,
                        self.semaphore.clone(),
                        self.attributes.multi_part_chunk_size,
                        self.attributes.multi_part_chunks_per_batch,
                        self.attributes.multi_part_parallelism,
                    )
                    .await
            }
        }
    }

    async fn record_description_handler(
        &self,
        source_oss_client: &OssClient,
        record: &RecordOption,
    ) -> Result<()> {
        match record.option {
            Opt::PUT => {
                let obj = match source_oss_client
                    .get_object(&self.source.bucket, &record.source_key)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        let service_err = e.into_service_error();
                        match service_err.is_no_such_key() {
                            true => {
                                return Ok(());
                            }
                            false => {
                                log::error!("{:?}", service_err);
                                return Err(service_err.into());
                            }
                        }
                    }
                };
                // let mut t_file = OpenOptions::new()
                //     .truncate(true)
                //     .create(true)
                //     .write(true)
                //     .open(&record.target_key)?;
                download_object(
                    obj,
                    // &mut t_file,
                    &record.target_key,
                    self.attributes.large_file_size,
                    self.attributes.multi_part_chunk_size,
                )
                .await?
            }
            Opt::REMOVE => {
                let _ = fs::remove_file(record.target_key.as_str());
            }
            _ => return Err(anyhow!("option unkown")),
        }
        Ok(())
    }
}
