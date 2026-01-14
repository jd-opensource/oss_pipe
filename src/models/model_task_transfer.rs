use crate::models::model_filters::LastModifyFilter;
use crate::models::model_s3::OSSDescription;
use crate::models::{
    model_task::ObjectStorage, model_task_default_parameters::TaskDefaultParameters,
};
use crate::tasks::{de_usize_from_str, se_usize_to_str};
use serde::{Deserialize, Serialize};

// #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
// #[serde(rename_all = "lowercase")]
// pub struct IncrementModeScan {
//     pub interval: u64,
// }

// impl Default for IncrementModeScan {
//     fn default() -> Self {
//         Self { interval: 3600 }
//     }
// }

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum IncrementMode {
    Notify,
    #[serde(untagged)]
    Scan {
        interval: u64,
    },
}

impl Default for IncrementMode {
    fn default() -> Self {
        IncrementMode::Notify
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TransferType {
    Full,
    Stock,
    Increment,
}

impl TransferType {
    pub fn is_full(&self) -> bool {
        match self {
            TransferType::Full => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_stock(&self) -> bool {
        match self {
            TransferType::Stock => true,
            _ => false,
        }
    }

    pub fn is_increment(&self) -> bool {
        match self {
            TransferType::Increment => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct TransferTask {
    #[serde(default = "TaskDefaultParameters::id_default")]
    pub task_id: String,
    #[serde(default = "TaskDefaultParameters::name_default")]
    pub name: String,
    pub source: ObjectStorage,
    pub target: ObjectStorage,
    pub attributes: TransferTaskAttributes,
}

impl Default for TransferTask {
    fn default() -> Self {
        Self {
            task_id: TaskDefaultParameters::id_default(),
            name: TaskDefaultParameters::name_default(),
            source: ObjectStorage::OSS(OSSDescription::default()),
            target: ObjectStorage::OSS(OSSDescription::default()),
            attributes: TransferTaskAttributes::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferTaskAttributes {
    /// 每批次处理的对象数量
    #[serde(default = "TaskDefaultParameters::objects_transfer_batch_default")]
    pub objects_transfer_batch: i32,
    /// 任务并行度
    #[serde(default = "TaskDefaultParameters::task_parallelism_default")]
    pub task_parallelism: usize,
    /// 元数据存储目录路径
    #[serde(default = "TaskDefaultParameters::meta_dir_default")]
    pub meta_dir: String,
    /// 目标已存在时是否跳过
    #[serde(default = "TaskDefaultParameters::target_exists_skip_default")]
    pub target_exists_skip: bool,
    /// 是否从检查点开始执行
    #[serde(default = "TaskDefaultParameters::start_from_checkpoint_default")]
    pub start_from_checkpoint: bool,
    /// 大文件大小阈值 (字节)
    #[serde(default = "TaskDefaultParameters::large_file_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub large_file_size: usize,
    /// 分片上传的块大小 (字节)
    #[serde(default = "TaskDefaultParameters::multi_part_chunk_size_default")]
    #[serde(serialize_with = "se_usize_to_str")]
    #[serde(deserialize_with = "de_usize_from_str")]
    pub multi_part_chunk_size: usize,
    /// 每批次处理的分片数量
    #[serde(default = "TaskDefaultParameters::multi_part_chunks_per_batch_default")]
    pub multi_part_chunks_per_batch: usize,
    /// 分片上传并行度
    #[serde(default = "TaskDefaultParameters::multi_part_parallelism_default")]
    pub multi_part_parallelism: usize,
    /// 分片上传最大并行度
    #[serde(default = "TaskDefaultParameters::multi_part_max_parallelism_default")]
    pub multi_part_max_parallelism: usize,
    /// 排除的文件/对象匹配模式列表
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub exclude: Option<Vec<String>>,
    /// 包含的文件/对象匹配模式列表
    #[serde(default = "TaskDefaultParameters::filter_default")]
    pub include: Option<Vec<String>>,
    /// 传输类型：全量/存量   Full,Stock
    #[serde(default = "TaskDefaultParameters::transfer_type_default")]
    pub transfer_type: TransferType,
    /// 最后修改时间过滤器配置
    #[serde(default = "TaskDefaultParameters::last_modify_filter_default")]
    pub last_modify_filter: Option<LastModifyFilter>,
    /// 对象列表批次，每批次写入列表文件的数量
    #[serde(default = "TaskDefaultParameters::objects_list_batch_default")]
    pub objects_list_batch: i32,
    /// 对象列表文件最大行数
    #[serde(default = "TaskDefaultParameters::objects_list_files_max_line_default")]
    pub objects_list_file_max_line: usize,
    /// 手动指定列表文件
    #[serde(default = "TaskDefaultParameters::objects_list_files_default")]
    pub objects_list_files: Option<Vec<String>>,
    /// 增量模式：scan 或 notify，文件系统支持 notify 模式，源端为对象存在只支持 scan 模式，当文件系统为共享存储，例如 nfs 时使用 scan 模式
    #[serde(default = "TaskDefaultParameters::increment_mode_default")]
    pub increment_mode: IncrementMode,
}

impl Default for TransferTaskAttributes {
    fn default() -> Self {
        Self {
            objects_transfer_batch: TaskDefaultParameters::objects_transfer_batch_default(),
            task_parallelism: TaskDefaultParameters::task_parallelism_default(),
            meta_dir: TaskDefaultParameters::meta_dir_default(),
            target_exists_skip: TaskDefaultParameters::target_exists_skip_default(),
            start_from_checkpoint: TaskDefaultParameters::target_exists_skip_default(),
            large_file_size: TaskDefaultParameters::large_file_size_default(),
            multi_part_chunk_size: TaskDefaultParameters::multi_part_chunk_size_default(),
            multi_part_chunks_per_batch: TaskDefaultParameters::multi_part_chunks_per_batch_default(
            ),
            multi_part_parallelism: TaskDefaultParameters::multi_part_parallelism_default(),
            multi_part_max_parallelism: TaskDefaultParameters::multi_part_max_parallelism_default(),
            exclude: TaskDefaultParameters::filter_default(),
            include: TaskDefaultParameters::filter_default(),
            transfer_type: TaskDefaultParameters::transfer_type_default(),
            last_modify_filter: TaskDefaultParameters::last_modify_filter_default(),
            objects_list_batch: TaskDefaultParameters::objects_list_batch_default(),
            objects_list_file_max_line: TaskDefaultParameters::objects_list_files_max_line_default(
            ),
            objects_list_files: TaskDefaultParameters::objects_list_files_default(),
            increment_mode: TaskDefaultParameters::increment_mode_default(),
        }
    }
}
