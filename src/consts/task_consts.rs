/// 对象序列文件名，用于记录对象列表文件的执行顺序
pub const OBJECTS_SEQUENCE_FILE: &'static str = "list_files/list_files_sequence";

/// 对象列表文件前缀，用于记录所有 list 文件的顺序，执行时依据文件顺序执行
pub const OBJECT_LIST_FILE_PREFIX: &'static str = "list_files/objects_list_";

/// 比较任务中源端对象列表文件前缀，用于存储源端对象信息
pub const COMPARE_SOURCE_OBJECT_LIST_FILE_PREFIX: &'static str = "compare_source_list_";

/// 传输任务检查点文件名，用于断点续传功能
pub const TRANSFER_CHECK_POINT_FILE: &'static str = "checkpoint_transfer.yml";

/// 比较任务检查点文件名，用于比较任务的断点续传
pub const COMPARE_CHECK_POINT_FILE: &'static str = "checkpoint_compare.yml";

/// 手动指定列表文件的检查点文件名，用于手动列表传输的断点续传
pub const MANUAL_LIST_CHECK_POINT_FILE: &'static str = "checkpoint_manual_list_transfer.yml";

/// 传输错误记录文件前缀，用于记录传输过程中发生的错误
pub const TRANSFER_ERROR_RECORD_PREFIX: &'static str = "error_records/transfer_error_record_";

/// 比较结果文件前缀，用于存储文件比较的结果
pub const COMPARE_RESULT_PREFIX: &'static str = "compare_results/compare_result_";

/// 偏移量文件前缀，用于记录文件读取的偏移位置
pub const OFFSET_PREFIX: &'static str = "offset_";

/// 通知文件前缀，用于增量同步中的文件变更通知
pub const NOTIFY_FILE_PREFIX: &'static str = "notify_";

pub const NOTIFY_SEQUENCE_FILE: &'static str = "notify/sequence";
pub const NOTIFY_FILES_PREFIX: &'static str = "notify/notify_";

/// 已删除文件前缀，用于标记已被删除的文件
pub const REMOVED_PREFIX: &'static str = "increment/removed_";

/// 已修改文件前缀，用于标记已被修改的文件
pub const MODIFIED_PREFIX: &'static str = "increment/modified_";

/// 下载临时文件后缀，用于标识正在下载中的临时文件
pub const DOWNLOAD_TMP_FILE_SUBFFIX: &'static str = ".filling_tmp";
