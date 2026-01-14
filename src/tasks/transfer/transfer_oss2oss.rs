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
        model_task_default_parameters::TaskDefaultParameters,
        model_task_transfer::{IncrementMode, TransferTaskAttributes},
    },
    s3::oss_client::{multipart_transfer_obj_paralle_by_range, OssClient},
    tasks::LogInfo,
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
use aws_smithy_types::{date_time::Format, DateTime};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Write},
    sync::{atomic::AtomicBool, Arc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Semaphore, task::JoinSet};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
// 定义一个结构体 TransferOss2Oss，用于描述从 OSS 到 OSS 的传输任务
pub struct TransferOss2Oss {
    // 任务 ID，默认值为 TaskDefaultParameters::id_default()
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    // 任务名称，默认值为 TaskDefaultParameters::name_default()
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    // 源 OSS 描述
    pub source: OSSDescription,
    // 目标 OSS 描述
    pub target: OSSDescription,
    // 传输任务属性
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferOss2Oss {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: OSSDescription::default(),
            target: OSSDescription::default(),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[async_trait]
impl TransferTaskActions for TransferOss2Oss {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>> {
        let client = self
            .source
            .gen_oss_client()
            .context(format!("{}:{}", file!(), line!()))?;
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
            let file_name = entry.file_name().to_str().unwrap();

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
                        let _ = executor.transfer_record_options(record_vec);
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
        let executor = TransferOss2OssRecordsExecutor {
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

        self.capture_removed_objects_to_file(&removed)
            .await
            .context(format!("{}:{}", file!(), line!()))?;
        self.capture_modified_objects_to_file(timestamp, &modified)
            .await
            .context(format!("{}:{}", file!(), line!()))?;

        merge_file(&modified, &removed, self.attributes.multi_part_chunk_size).context(format!(
            "{}:{}",
            file!(),
            line!()
        ))?;
        fs::rename(&removed, &modified).context(format!("{}:{}", file!(), line!()))?;
        let file_desc =
            FileDescription::from_file(&modified).context(format!("{}:{}", file!(), line!()))?;

        let log_info = LogInfo {
            task_id: self.task_id.clone(),
            msg: "capture changed object".to_string(),
            additional: Some(file_desc.clone()),
        };
        log::info!("{:?} ", log_info);
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
        increment_mod: IncrementMode,
    ) {
        let interval = match increment_mod {
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
            for (idx, line) in lines.enumerate() {
                if let Result::Ok(line_str) = line {
                    let len = line_str.bytes().len() + "\n".bytes().len();

                    let record = match from_str::<RecordOption>(&line_str) {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("{:?}", e);
                            err_occur.store(true, std::sync::atomic::Ordering::SeqCst);
                            stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                            return;
                        }
                    };
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
                    || idx.eq(&TryInto::<usize>::try_into(modified.total_lines - 1).unwrap())
                        && vec_keys.len() > 0
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

            checkpoint.executing_file_position = FilePosition {
                // Todo 详细了解该函数功能后调整 file_num,
                file_num: -1,
                offset: modified.size.try_into().unwrap(),
                line_num: modified.total_lines,
            };
            checkpoint.executing_file = modified.clone();
            checkpoint.last_scan_timestamp = i128::from(now.as_secs());

            if let Err(e) = checkpoint.save_to(&checkpoint_path) {
                log::error!("{:?}", e);
                return;
            }

            let _ = fs::remove_file(&modified.path);

            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        }
        pd.finish();
    }
}

impl TransferOss2Oss {
    // 获取 source 端变更的 objects，时间戳大于等于上次抓取的时间戳
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
            .open(&to_file)?;
        let regex_filter =
            RegexFilter::from_vec_option(&self.attributes.exclude, &self.attributes.include)?;

        let last_modify_filter = LastModifyFilter {
            filter_type: LastModifyFilterType::Greater,
            timestamp: last_capture_timestamp,
        };

        let source_client = self.source.gen_oss_client()?;

        // 筛选源对象，lastmodify 大于等于时间戳并转换为 RecordDescription 格式
        let mut persist_modified_objects = |source_objects: Vec<Object>| -> Result<()> {
            for obj in source_objects {
                if let Some(source_key) = obj.key() {
                    if let Some(ref f) = regex_filter {
                        if !f.passed(source_key) {
                            continue;
                        }
                    };
                    if let Some(d) = obj.last_modified() {
                        if last_modify_filter.passed(usize::try_from(d.secs())?) {
                            let mut target_key = "".to_string();
                            if let Some(p) = &self.target.prefix {
                                target_key.push_str(p);
                            }
                            target_key.push_str(source_key);

                            let record = RecordOption {
                                source_key: source_key.to_string(),
                                target_key,
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
            modified_file.flush()?;
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
        create_parent_dir(to_file).context(format!("{}:{}", file!(), line!()))?;
        let mut removed_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(to_file)
            .context(format!("{}:{}", file!(), line!()))?;
        let source_client =
            self.source
                .gen_oss_client()
                .context(format!("{}:{}", file!(), line!()))?;
        let target_client =
            self.target
                .gen_oss_client()
                .context(format!("{}:{}", file!(), line!()))?;

        // 获取目标所有 object 与源对比得到已删除的 object 并写入文件
        let target_resp = target_client
            .list_objects(
                &self.target.bucket,
                self.target.prefix.clone(),
                self.attributes.objects_transfer_batch,
                None,
            )
            .await?;

        let mut target_token = target_resp.next_token;

        if let Some(objects) = target_resp.object_list {
            self.persist_moved_objects(objects, &source_client, &mut removed_file)
                .await?;
        }

        while target_token.is_some() {
            let resp = target_client
                .list_objects(
                    &self.target.bucket,
                    self.target.prefix.clone(),
                    self.attributes.objects_transfer_batch,
                    target_token,
                )
                .await?;
            if let Some(objects) = resp.object_list {
                self.persist_moved_objects(objects, &source_client, &mut removed_file)
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;
            }
            target_token = resp.next_token;
        }
        removed_file
            .flush()
            .context(format!("{}:{}", file!(), line!()))?;
        Ok(())
    }

    async fn persist_moved_objects(
        &self,
        objects: Vec<Object>,
        source_client: &OssClient,
        to_file: &mut File,
    ) -> Result<()> {
        for obj in objects {
            if let Some(target_key) = obj.key() {
                let mut source_key = "".to_string();
                if let Some(p) = &self.target.prefix {
                    let key = match p.ends_with("/") {
                        true => &target_key[p.len()..],
                        false => &target_key[p.len() + 1..],
                    };
                    source_key.push_str(key);
                } else {
                    source_key.push_str(target_key);
                };

                if !source_client
                    .object_exists(&self.source.bucket, &source_key)
                    .await?
                {
                    let record = RecordOption {
                        source_key,
                        target_key: target_key.to_string(),
                        list_file_path: "".to_string(),
                        list_file_position: FilePosition::default(),
                        option: Opt::REMOVE,
                    };

                    let record_str = struct_to_json_string(&record)?;
                    let _ = to_file.write_all(record_str.as_bytes());
                    let _ = to_file.write_all("\n".as_bytes());
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TransferOss2OssRecordsExecutor {
    pub source: OSSDescription,
    pub target: OSSDescription,
    pub stop_mark: Arc<AtomicBool>,
    pub err_occur: Arc<AtomicBool>,
    pub semaphore: Arc<Semaphore>,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
    pub attributes: TransferTaskAttributes,
    pub list_file_path: String,
}

#[async_trait]
impl TransferExecutor for TransferOss2OssRecordsExecutor {
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

        {
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(error_file_name.as_str())?;
        }

        let source_client = self.source.gen_oss_client()?;
        let target_client = self.target.gen_oss_client()?;
        let s_c = Arc::new(source_client);
        let t_c = Arc::new(target_client);
        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst)
                || self.err_occur.load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }

            let mut target_key = match self.target.prefix.clone() {
                Some(s) => s,
                None => "".to_string(),
            };
            target_key.push_str(&record.key);

            self.offset_map.insert(
                offset_key.clone(),
                FilePosition {
                    file_num: record.file_num,
                    offset: record.offset,
                    line_num: record.line_num,
                },
            );

            if let Err(e) = self
                .listed_record_handler(&record, &s_c, &t_c, &target_key)
                .await
            {
                let record_option = RecordOption {
                    source_key: record.key.clone(),
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
                );
                log::error!("{:?} {:?}", e, record_option);
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
            .open(error_file_name.as_str())
            .context(format!("{}:{}", file!(), line!()))?;

        drop(error_file);

        let s_client = self.source.gen_oss_client()?;
        let t_client = self.target.gen_oss_client()?;
        let s_c = Arc::new(s_client);
        let t_c = Arc::new(t_client);

        for record in records {
            if self.stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            if let Err(e) = self.record_description_handler(&s_c, &t_c, &record).await {
                record.handle_error(
                    self.stop_mark.clone(),
                    self.err_occur.clone(),
                    &error_file_name,
                );
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.stop_mark
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                log::error!("{:?}", e);

                // 记录执行文件位置
                self.offset_map
                    .insert(offset_key.clone(), record.list_file_position.clone());
            };
        }
        self.offset_map.remove(&offset_key);

        let error_file = match File::open(&error_file_name) {
            Ok(f) => f,
            Err(e) => {
                self.err_occur
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                // log::error!("{:?}", e);
                return Err(anyhow!(e).context(format!("{}:{}", file!(), line!())));
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

impl TransferOss2OssRecordsExecutor {
    //Todo
    // 尝试 source_oss、target_oss 参数 使用 Arc<Client>
    async fn listed_record_handler(
        &self,
        record: &ListedRecord,
        source_oss: &Arc<OssClient>,
        target_oss: &Arc<OssClient>,
        target_key: &str,
    ) -> Result<()> {
        let s_obj_output = match source_oss
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

        // 目标 object 存在则不推送
        if self.attributes.target_exists_skip {
            let target_obj_exists = target_oss
                .object_exists(self.target.bucket.as_str(), target_key)
                .await
                .context(format!("{}:{}", file!(), line!()))?;
            if target_obj_exists {
                return Ok(());
            }
        }

        let content_len = match s_obj_output.content_length() {
            Some(l) => l,
            None => return Err(anyhow!("content length is None")),
        };
        let content_len_usize: usize = content_len.try_into()?;

        let expr = match s_obj_output.expires_string {
            Some(str) => {
                let date = DateTime::from_str(&str, Format::DateTime).context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;
                Some(date)
            }
            None => None,
        };

        return match content_len_usize.le(&self.attributes.large_file_size) {
            true => {
                target_oss
                    .upload_object_bytes(
                        self.target.bucket.as_str(),
                        target_key,
                        expr,
                        s_obj_output.body,
                    )
                    .await
            }
            false => {
                multipart_transfer_obj_paralle_by_range(
                    source_oss.clone(),
                    &self.source.bucket,
                    record.key.as_str(),
                    target_oss.clone(),
                    &self.target.bucket,
                    target_key,
                    expr,
                    self.semaphore.clone(),
                    self.attributes.multi_part_chunk_size,
                    self.attributes.multi_part_chunks_per_batch,
                    self.attributes.multi_part_parallelism,
                )
                .await
            }
        };
    }

    async fn record_description_handler(
        &self,
        source_oss: &Arc<OssClient>,
        target_oss: &Arc<OssClient>,
        record: &RecordOption,
    ) -> Result<()> {
        // 目标 object 存在则不推送
        if self.attributes.target_exists_skip {
            match target_oss
                .object_exists(self.target.bucket.as_str(), &record.target_key)
                .await
            {
                Ok(b) => {
                    if b {
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        match record.option {
            Opt::PUT => {
                let s_obj = match source_oss
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

                let content_len = match s_obj.content_length() {
                    Some(l) => l,
                    None => return Err(anyhow!("content length is None")),
                };
                let content_len_usize: usize =
                    content_len
                        .try_into()
                        .context(format!("{}:{}", file!(), line!()))?;

                let expr =
                    match s_obj.expires_string {
                        Some(str) => {
                            let date = DateTime::from_str(&str, Format::DateTime)
                                .context(format!("{}:{}", file!(), line!()))?;
                            Some(date)
                        }
                        None => None,
                    };

                return match content_len_usize.le(&self.attributes.large_file_size) {
                    true => {
                        target_oss
                            .upload_object_bytes(
                                self.target.bucket.as_str(),
                                &record.target_key,
                                expr,
                                s_obj.body,
                            )
                            .await
                    }
                    false => {
                        multipart_transfer_obj_paralle_by_range(
                            source_oss.clone(),
                            &self.source.bucket,
                            &record.source_key,
                            target_oss.clone(),
                            &self.target.bucket,
                            &record.target_key,
                            expr,
                            self.semaphore.clone(),
                            self.attributes.multi_part_chunk_size,
                            self.attributes.multi_part_chunks_per_batch,
                            self.attributes.multi_part_parallelism,
                        )
                        .await
                    }
                };
            }
            Opt::REMOVE => {
                target_oss
                    .remove_object(&self.target.bucket, &record.target_key)
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;
            }
            _ => return Err(anyhow!("option unkown")),
        }
        Ok(())
    }
}
