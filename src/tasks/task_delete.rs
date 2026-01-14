use super::{gen_file_path, TaskStatusSaver};
use crate::{
    checkpoint::{get_task_checkpoint, ListedRecord},
    commons::{count_file_bytes, count_file_lines, prompt_processbar, RegexFilter},
    consts::task_consts::{OBJECTS_SEQUENCE_FILE, OFFSET_PREFIX, TRANSFER_CHECK_POINT_FILE},
    models::{
        model_checkpoint::{FileDescription, FilePosition},
        model_s3::OSSDescription,
        model_task::{ObjectStorage, TaskStage},
        model_task_delete_bucket::TaskDeleteBucket,
    },
};
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::types::ObjectIdentifier;
use dashmap::DashMap;
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, Seek, SeekFrom},
    sync::{atomic::AtomicBool, Arc},
};
use tokio::task::{self, JoinSet};

//Todo
// 增加 start_from_checkpoint arttribute
impl TaskDeleteBucket {
    pub async fn execute(&self) -> Result<()> {
        // let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let task_stop_mark = Arc::new(AtomicBool::new(false));
        let task_err_occur = Arc::new(AtomicBool::new(false));
        let offset_map = Arc::new(DashMap::<String, FilePosition>::new());

        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        let mut list_file_position = FilePosition::default();

        let oss_d = match self.target.clone() {
            ObjectStorage::Local(_) => {
                return Err(anyhow::anyhow!("parse oss description error"));
            }
            ObjectStorage::OSS(d) => d,
        };

        let (obj_list_files, current_list_file_position) =
            self.generate_list_multi_files()
                .await
                .context(format!("{}:{}", file!(), line!()))?;
        if obj_list_files.len().eq(&0) {
            return Ok(());
        }

        let file_num_usize = TryInto::<usize>::try_into(current_list_file_position.file_num)?;

        let file_position = current_list_file_position.clone();

        let o_d = oss_d.clone();

        // sys_set 用于执行 checkpoint、notify 等辅助任务
        let mut sys_set = JoinSet::new();
        // execut_set 用于执行任务
        let mut execut_set = JoinSet::new();

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

        let mut vec_record = vec![];

        for exec_file_desc in obj_list_files.iter().skip(file_num_usize) {
            // 按 file_position seek file
            let seek_offset = TryInto::<u64>::try_into(file_position.offset)
                .context(format!("{}:{}", file!(), line!()))
                .unwrap();
            let mut exec_file = File::open(exec_file_desc.path.as_str())
                .context(format!("{}:{}", file!(), line!()))
                .unwrap();
            exec_file
                .seek(SeekFrom::Start(seek_offset))
                .context(format!("{}:{}", file!(), line!()))
                .unwrap();

            let lines: io::Lines<io::BufReader<File>> = io::BufReader::new(exec_file).lines();
            let exec_lines = exec_file_desc.total_lines - file_position.line_num;

            // let lines = io::BufReader::new(objects_list_file).lines();
            for (line_idx, line) in lines.enumerate() {
                if task_stop_mark.load(std::sync::atomic::Ordering::SeqCst) {
                    return Ok(());
                }
                match line {
                    Ok(key) => {
                        let len = key.bytes().len() + "\n".bytes().len();
                        let record = ListedRecord {
                            file_num: 0,
                            key,
                            offset: list_file_position.offset,
                            line_num: list_file_position.line_num,
                        };
                        vec_record.push(record);

                        list_file_position.offset += len;
                        list_file_position.line_num += 1;
                    }
                    Err(e) => {
                        log::error!("{:?}", e);
                        task_stop_mark.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }

                if vec_record
                    .len()
                    .to_string()
                    .eq(&self.attributes.objects_per_batch.to_string())
                    || line_idx.eq(&TryInto::<usize>::try_into(exec_lines - 1).unwrap())
                        && vec_record.len() > 0
                {
                    if task_stop_mark.load(std::sync::atomic::Ordering::Relaxed) {
                        return Ok(());
                    };
                    while execut_set.len() >= self.attributes.task_parallelism {
                        execut_set.join_next().await;
                    }

                    let del = DeleteBucketExecutor {
                        source: o_d.clone(),
                        offset_map: offset_map.clone(),
                    };
                    let keys = vec_record.clone();
                    let sm: Arc<AtomicBool> = task_stop_mark.clone();
                    let eo = task_err_occur.clone();

                    execut_set.spawn(async move {
                        match del.delete_listed_records(keys).await {
                            Ok(_) => log::info!("records removed"),
                            Err(e) => {
                                sm.store(true, std::sync::atomic::Ordering::Relaxed);
                                eo.store(true, std::sync::atomic::Ordering::Relaxed);
                                log::error!("{}", e);
                            }
                        }
                    });

                    vec_record.clear();
                }
            }
        }

        while execut_set.len() > 0 {
            execut_set.join_next().await;
        }
        // 配置停止 offset save 标识为 true
        task_stop_mark.store(true, std::sync::atomic::Ordering::Relaxed);
        if task_err_occur.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow!("compare task error"));
        }

        while sys_set.len() > 0 {
            task::yield_now().await;
            sys_set.join_next().await;
        }

        Ok(())
    }

    async fn generate_list_multi_files(&self) -> Result<(Vec<FileDescription>, FilePosition)> {
        let check_point_file = gen_file_path(
            self.attributes.meta_dir.as_str(),
            TRANSFER_CHECK_POINT_FILE,
            "",
        );

        match self.attributes.start_from_checkpoint {
            true => {
                let checkpoint = get_task_checkpoint(check_point_file.as_str())
                    .context(format!("{}:{}", file!(), line!()))?;
                let list_files_desc = self.gen_target_object_list_files_desc().context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;
                let list_file_position = checkpoint.executing_file_position.clone();
                Ok((list_files_desc, list_file_position))
            }
            false => {
                let pd = prompt_processbar("Generating object list ...");
                let _ = fs::remove_dir_all(self.attributes.meta_dir.as_str());

                let oss_d = match self.target.clone() {
                    ObjectStorage::Local(_) => {
                        return Err(anyhow::anyhow!("parse oss description error"));
                    }
                    ObjectStorage::OSS(d) => d,
                };

                let target_client =
                    oss_d
                        .gen_oss_client()
                        .context(format!("{}:{}", file!(), line!()))?;
                let regex_filter = RegexFilter::from_vec_option(
                    &self.attributes.exclude,
                    &self.attributes.include,
                )?;
                let list_files_desc = target_client
                    .append_object_list_to_multi_files(
                        oss_d.bucket.clone(),
                        oss_d.prefix.clone(),
                        self.attributes.objects_per_batch,
                        regex_filter,
                        self.attributes.last_modify_filter,
                        &self.attributes.meta_dir,
                        self.attributes.objects_list_files_max_line,
                    )
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;

                pd.finish_with_message("object list generated");
                Ok((list_files_desc, FilePosition::default()))
            }
        }
    }

    pub fn gen_target_object_list_files_desc(&self) -> Result<Vec<FileDescription>> {
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

#[derive(Debug, Clone)]
pub struct DeleteBucketExecutor {
    pub source: OSSDescription,
    pub offset_map: Arc<DashMap<String, FilePosition>>,
}

impl DeleteBucketExecutor {
    pub async fn delete_listed_records(&self, records: Vec<ListedRecord>) -> Result<()> {
        let subffix = records[0].offset.to_string();
        let mut offset_key = OFFSET_PREFIX.to_string();
        offset_key.push_str(&subffix);

        let source_client = self.source.gen_oss_client()?;
        let s_c = Arc::new(source_client);

        // 插入文件 offset 记录
        self.offset_map.insert(
            offset_key.clone(),
            FilePosition {
                file_num: records[0].file_num,
                offset: records[0].offset,
                line_num: records[0].line_num,
            },
        );

        let mut del_objs = vec![];

        for record in records {
            let obj_identifier = ObjectIdentifier::builder().key(record.key).build()?;
            del_objs.push(obj_identifier);
        }

        s_c.remove_objects(&self.source.bucket, del_objs).await?;

        self.offset_map.remove(&offset_key);
        Ok(())
    }
}
