use crate::{
    commons::{byte_size_str_to_usize, byte_size_usize_to_str, struct_to_yaml_string},
    models::model_task::Task,
    tasks::{log_info, LogInfo, MSG_TASK_EXECUTED_OK},
};
use anyhow::Result;
use serde::{
    de::{self},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snowflake::SnowflakeIdGenerator;
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
/// 表示分析结果的结构体，包含最大值和最小值
pub struct AnalyzedResult {
    /// 分析结果中的最大值
    pub max: i128,
    /// 分析结果中的最小值
    pub min: i128,
}

impl Task {
    /// 执行任务的主要方法
    ///
    /// 该方法根据任务类型（Transfer、DeleteBucket、Compare）执行相应的操作。
    /// 所有任务执行都会记录开始时间，并在完成后记录执行耗时。
    ///
    /// # 任务类型说明
    /// - `Transfer`: 数据传输任务，用于在不同存储系统间传输文件
    /// - `DeleteBucket`: 删除存储桶任务，用于清空或删除指定的存储桶
    /// - `Compare`: 比较任务，用于比较源端和目标端的数据一致性
    ///
    /// # 错误处理
    /// 所有任务执行过程中的错误都会被捕获并记录到日志中，不会向上传播
    pub async fn execute(&self) {
        // 记录任务开始执行的时间，用于计算总执行耗时
        let now = Instant::now();
        log::debug!("test debug");

        // 根据任务类型执行相应的操作
        match self {
            // 处理数据传输任务
            Task::Transfer(transfer) => {
                // 记录传输任务的详细信息
                tracing::info!(task = ?transfer);

                // 异步执行数据传输操作
                match transfer.start_transfer().await {
                    Ok(_) => {
                        // 传输成功，记录成功日志和执行耗时
                        log_info(
                            transfer.task_id.clone(),
                            MSG_TASK_EXECUTED_OK.to_string(),
                            Some(now.elapsed()),
                        );
                    }
                    Err(e) => {
                        // 传输失败，记录错误信息
                        log::error!("{:?}", e);
                    }
                }
            }
            // 处理删除存储桶任务
            Task::DeleteBucket(truncate) => {
                // 记录删除任务开始信息，包含任务的 YAML 格式配置
                log::info!(
                    "Truncate Task Start:\n{}",
                    struct_to_yaml_string(truncate).unwrap()
                );

                // 异步执行删除操作
                match truncate.execute().await {
                    Ok(_) => {
                        // 删除成功，构造并记录成功日志
                        let log_info = LogInfo {
                            task_id: truncate.task_id.clone(),
                            msg: "execute ok!".to_string(),
                            additional: Some(now.elapsed()),
                        };
                        log::info!("{:?}", log_info)
                    }
                    Err(e) => {
                        // 删除失败，记录错误信息
                        log::error!("{:?}", e);
                    }
                }
            }
            // 处理数据比较任务
            Task::Compare(compare) => {
                // 记录比较任务开始信息，包含任务的 YAML 格式配置
                log::info!(
                    "Compare Task Start:\n{}",
                    struct_to_yaml_string(compare).unwrap()
                );

                // 异步执行数据比较操作
                match compare.start_compare().await {
                    Ok(_) => {
                        // 比较成功，构造并记录成功日志
                        let log_info = LogInfo {
                            task_id: compare.task_id.clone(),
                            msg: "execute ok!".to_string(),
                            additional: Some(now.elapsed()),
                        };
                        log::info!("{:?}", log_info)
                    }
                    Err(e) => {
                        // 比较失败，记录错误信息
                        log::error!("{:?}", e);
                    }
                }
            }
        }
    }
}

pub fn de_usize_from_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    byte_size_str_to_usize(&s).map_err(de::Error::custom)
}

pub fn se_usize_to_str<S>(v: &usize, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let size = byte_size_usize_to_str(*v);
    serializer.serialize_str(size.as_str())
}

pub fn task_id_generator() -> i64 {
    let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
    let id = id_generator_generator.real_time_generate();
    id
}

#[cfg(target_family = "unix")]
pub fn gen_file_path(dir: &str, file_prefix: &str, file_subffix: &str) -> String {
    let mut file_name = dir.to_string();
    if dir.ends_with("/") {
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    } else {
        file_name.push_str("/");
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    }
    file_name
}

#[cfg(target_family = "windows")]
pub fn gen_file_path(dir: &str, file_prefix: &str, file_subffix: &str) -> String {
    let mut file_name = dir.to_string();
    if dir.ends_with("\\") {
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    } else {
        file_name.push_str("\\");
        file_name.push_str(file_prefix);
        file_name.push_str(file_subffix);
    }
    file_name
}
