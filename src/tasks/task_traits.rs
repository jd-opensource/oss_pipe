use crate::{
    checkpoint::{ListedRecord, RecordOption},
    models::{
        model_checkpoint::{FileDescription, FilePosition},
        model_task_transfer::IncrementMode,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{sync::Semaphore, task::JoinSet};

#[async_trait]
pub trait TransferTaskActions {
    async fn analyze_source(&self) -> Result<DashMap<String, i128>>;

    // Todo 新增 skipe_error 逻辑，用于跳过已记录的错误
    // 替换 error_record_retry，主要逻辑，当 transfer 过程出现错误时，核对记录是否被记录，若未被记录，则记录错误，并返回错误，停止任务
    // 若已被记录，则跳过错误，继续执行

    // 错误记录重试
    async fn error_record_retry(
        &self,
        stop_mark: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
    ) -> Result<()>;

    fn gen_transfer_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
        list_file_path: String,
    ) -> Arc<dyn TransferExecutor + Send + Sync>;

    // 生成对象列表
    async fn list_objects_to_multi_files(&self, meta_dir: &str) -> Result<Vec<FileDescription>>;

    // 以 target 为基础，抓取变动 object
    // 扫描 target storage，source 不存在为 removed object
    // 按时间戳扫描 source storage，大于指定时间戳的 object 为 removed objects
    async fn changed_object_capture_based_target(
        &self,
        timestamp: usize,
    ) -> Result<FileDescription>;

    // 执行增量前置操作，例如启动 notify 线程，记录 last modify 时间戳等
    async fn increment_prelude(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        notify_file_path: Option<String>,
        // assistant: Arc<Mutex<IncrementAssistant>>,
    ) -> Result<()>;

    // 执行增量任务
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
        increment_mode: IncrementMode,
    );
}

#[async_trait]
pub trait TransferExecutor {
    async fn transfer_listed_records(&self, records: Vec<ListedRecord>) -> Result<()>;
    async fn transfer_record_options(&self, records: Vec<RecordOption>) -> Result<()>;
}

#[async_trait]
pub trait CompareTaskActions {
    async fn list_objects_to_multi_files(&self, meta_dir: &str) -> Result<Vec<FileDescription>>;

    fn gen_compare_executor(
        &self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        semaphore: Arc<Semaphore>,
        offset_map: Arc<DashMap<String, FilePosition>>,
    ) -> Arc<dyn CompareExecutor + Send + Sync>;
}

#[async_trait]
pub trait CompareExecutor {
    async fn compare_listed_records(&self, records: Vec<ListedRecord>) -> Result<()>;
    fn error_occur(&self);
}
