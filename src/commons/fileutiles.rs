use super::{
    new_analyze_map,
    rand_util::{rand_path, rand_string},
    size_distributed, RegexFilter,
};
use crate::{
    // checkpoint::FileDescription,
    consts::task_consts::{OBJECTS_SEQUENCE_FILE, OBJECT_LIST_FILE_PREFIX},
    models::{model_checkpoint::FileDescription, model_filters::LastModifyFilter},
    tasks::gen_file_path,
};
use anyhow::{Context, Result};
use dashmap::DashMap;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, LineWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};
use time::OffsetDateTime;
use tokio::{sync::Mutex, task::JoinSet};
use walkdir::{DirEntry, WalkDir};

pub struct FileOperationBuilder {
    path: PathBuf,
    create_parent: bool,
    append_mode: bool,
}

impl FileOperationBuilder {
    // pub fn new(path: &str) -> Self {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            create_parent: false,
            append_mode: false,
        }
    }

    pub fn with_parent_creation(mut self) -> Self {
        self.create_parent = true;
        self
    }

    pub fn with_append_mode(mut self) -> Self {
        self.append_mode = true;
        self
    }

    pub fn build_file(self) -> Result<File> {
        if self.create_parent {
            create_parent_dir(&self.path)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(self.append_mode)
            .truncate(!self.append_mode)
            .open(&self.path)?;
        Ok(file)
    }

    pub fn build_line_writer(self) -> Result<LineWriter<File>> {
        let file = self.build_file()?;

        Ok(LineWriter::new(file))
    }
}

/// 文件分片
#[derive(Debug, Clone)]
pub struct FilePart {
    /// 分片号
    pub part_num: i32,
    /// 分片在文件中的偏移量
    pub offset: u64,
}

/// 读取文件的所有行，返回一个迭代器
///
/// # 参数
/// * `filename` - 文件路径
///
/// # 返回值
/// 返回文件行的迭代器，如果文件打开失败则返回错误
pub fn read_lines<P>(filename: P) -> Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

/// 复制文件，根据文件大小选择单次复制或分块复制
///
/// # 参数
/// * `source` - 源文件路径
/// * `target` - 目标文件路径
/// * `multi_parts_size` - 多部分复制的阈值大小
/// * `chunk_size` - 分块大小
///
/// # 返回值
/// 复制成功返回 Ok(())，失败返回错误
pub fn copy_file(
    source: &str,
    target: &str,
    multi_parts_size: usize,
    chunk_size: usize,
) -> Result<()> {
    let f_source = OpenOptions::new().read(true).open(source)?;
    let file_target_builder = FileOperationBuilder::new(target).with_parent_creation();

    // let mut f_target = OpenOptions::new()
    //     .create(true)
    //     .write(true)
    //     .truncate(true)
    //     .open(target)?;
    let mut f_target = file_target_builder.build_file()?;
    let len = f_source.metadata()?.len();
    let len_usize = TryInto::<usize>::try_into(len)?;
    match len_usize.gt(&multi_parts_size) {
        true => {
            multi_parts_copy_file(source, target, chunk_size)?;
        }
        false => {
            let data = fs::read(source)?;
            f_target.write_all(&data)?;
            f_target.flush()?;
        }
    }
    Ok(())
}

/// 分块复制文件，适用于大文件的复制操作
///
/// # 参数
/// * `source` - 源文件路径
/// * `target` - 目标文件路径
/// * `chunk_size` - 每次读取的块大小
///
/// # 返回值
/// 复制成功返回 Ok(())，失败返回错误
pub fn multi_parts_copy_file(source: &str, target: &str, chunk_size: usize) -> Result<()> {
    let mut f_source = OpenOptions::new().read(true).open(source)?;
    let target_file_builder = FileOperationBuilder::new(target).with_parent_creation();
    // let mut f_target = OpenOptions::new()
    //     .create(true)
    //     .write(true)
    //     .truncate(true)
    //     .open(target)?;
    let mut f_target = target_file_builder.build_file()?;

    loop {
        let mut buffer = vec![0; chunk_size];
        let read_count = f_source.read(&mut buffer)?;
        let buf = &buffer[..read_count];
        f_target.write_all(&buf)?;
        if read_count != chunk_size {
            break;
        }
    }
    f_target.flush()?;
    Ok(())
}

#[allow(dead_code)]
fn remove_dir_contents<P: AsRef<Path>>(path: P) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if entry.file_type()?.is_dir() {
            remove_dir_contents(&path)?;
            fs::remove_dir(path)?;
        } else {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

/// 分析文件夹中文件的大小分布情况
///
/// # 参数
/// * `folder` - 要分析的文件夹路径
/// * `regex_filter` - 可选的正则表达式过滤器
/// * `last_modify_filter` - 可选的最后修改时间过滤器
///
/// # 返回值
/// 返回文件大小分布的映射表，键为大小范围，值为文件数量
pub fn analyze_folder_files_size(
    folder: &str,
    regex_filter: Option<RegexFilter>,
    last_modify_filter: Option<LastModifyFilter>,
) -> Result<DashMap<String, i128>> {
    let size_map = new_analyze_map();

    for entry in WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            if p.eq(folder) {
                continue;
            }

            if let Some(f) = &regex_filter {
                if !f.passed(p) {
                    continue;
                }
            }

            if let Some(f) = &last_modify_filter {
                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();

                if !f.passed(usize::try_from(modified_time).unwrap()) {
                    continue;
                }
            }

            let obj_size = i128::from(entry.metadata()?.len());
            let key = size_distributed(obj_size);
            let mut size = match size_map.get(&key) {
                Some(m) => *m.value(),
                None => 0,
            };
            size += 1;
            size_map.insert(key, size);
        };
    }
    Ok(size_map)
}

pub async fn analyze_folder_files_size_parallel(
    folder: &str,
    regex_filter: Option<RegexFilter>,
    last_modify_filter: Option<LastModifyFilter>,
    batch_size: usize,
    parallelism: usize,
) -> Result<DashMap<String, i128>> {
    println!("invock");
    let arc_size_map = Arc::new(Mutex::new(new_analyze_map()));
    let mut joinset = JoinSet::new();
    let mut entries = vec![];

    for entry in WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            if p.eq(folder) {
                continue;
            }
            entries.push(entry);
        };

        if entries.len() >= batch_size {
            let map = arc_size_map.clone();
            let r = regex_filter.clone();
            let l = last_modify_filter.clone();
            let e = entries.clone();
            while joinset.len() >= parallelism {
                joinset.join_next().await;
            }
            joinset.spawn(async move {
                if let Err(e) = file_size_distributed(map, r, l, e).await {
                    log::error!("{:?}", e);
                };
            });
            entries.clear();
        }
    }

    if entries.len() > 0 {
        let map = arc_size_map.clone();
        let r = regex_filter.clone();
        let l = last_modify_filter.clone();
        let e = entries.clone();
        while joinset.len() >= parallelism {
            joinset.join_next().await;
        }
        joinset.spawn(async move {
            if let Err(e) = file_size_distributed(map, r, l, e).await {
                log::error!("{:?}", e);
            };
        });
    }

    while let Some(_) = joinset.join_next().await {}
    let map = arc_size_map.lock().await;
    Ok((*map).clone())
}

async fn file_size_distributed(
    size_map: Arc<Mutex<DashMap<String, i128>>>,
    regex_filter: Option<RegexFilter>,
    last_modify_filter: Option<LastModifyFilter>,
    entries: Vec<DirEntry>,
) -> Result<()> {
    for entry in entries {
        if let Some(p) = entry.path().to_str() {
            if let Some(f) = &regex_filter {
                if !f.passed(p) {
                    continue;
                }
            }

            if let Some(f) = &last_modify_filter {
                let modified_time = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();

                if !f.passed(usize::try_from(modified_time).unwrap()) {
                    continue;
                }
            }

            let obj_size = i128::from(entry.metadata()?.len());
            let key = size_distributed(obj_size);

            let map = size_map.lock().await;
            let mut current = map.entry(key.clone()).or_insert(0);
            *current += 1;
        };
    }
    Ok(())
}

// 遍历目录，将目录下的所有文件的相对路径写入多个文件
/// 扫描文件夹中的文件并将文件路径写入多个列表文件
///
/// # 参数
/// * `folder` - 要扫描的文件夹路径
/// * `regex_filter` - 可选的正则表达式过滤器
/// * `last_modify_filter` - 可选的最后修改时间过滤器
/// * `meta_dir` - 元数据目录路径
/// * `file_max_lines` - 每个列表文件的最大行数
///
/// # 返回值
/// 返回生成的文件描述列表
pub fn scan_folder_files_to_multi_files(
    folder: &str,
    regex_filter: Option<RegexFilter>,
    last_modify_filter: Option<LastModifyFilter>,
    meta_dir: &str,
    file_max_lines: usize,
) -> Result<Vec<FileDescription>> {
    let mut file_lines = 0;
    let mut file_num = 0;
    let sequence_file_name = gen_file_path(meta_dir, OBJECTS_SEQUENCE_FILE, "");
    let sequence_file_builder = FileOperationBuilder::new(&sequence_file_name)
        .with_append_mode()
        .with_parent_creation();
    let mut sequence_line_writer = sequence_file_builder.build_line_writer()?;

    // 创建 object list 文件
    let mut file_subfix = "".to_string();
    file_subfix.push_str(&file_num.to_string());

    let input_file_name = gen_file_path(meta_dir, OBJECT_LIST_FILE_PREFIX, &file_subfix);
    let input_file_builder = FileOperationBuilder::new(&input_file_name)
        .with_append_mode()
        .with_parent_creation();
    let mut input_file_writer = input_file_builder.build_line_writer()?;

    // 记录第一个文件列表
    let _ = sequence_line_writer.write_all(input_file_name.clone().as_bytes());
    let _ = sequence_line_writer.write_all("\n".as_bytes());

    // 遍历目录并将文件路径写入文件
    for entry in WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        if let Some(p) = entry.path().to_str() {
            if p.eq(folder) {
                continue;
            }

            if let Some(f) = last_modify_filter {
                let modified_time = entry
                    .metadata()
                    .context(format!("{}:{}", file!(), line!()))?
                    .modified()
                    .context(format!("{}:{}", file!(), line!()))?
                    .duration_since(UNIX_EPOCH)
                    .context(format!("{}:{}", file!(), line!()))?
                    .as_secs();
                if !f.passed(usize::try_from(modified_time).context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?) {
                    continue;
                }
            }

            let key = match folder.ends_with("/") {
                true => &p[folder.len()..],
                false => &p[folder.len() + 1..],
            };

            if let Some(ref f) = regex_filter {
                // ToDo 待理顺逻辑
                if !f.passed(key) {
                    continue;
                }
            }

            let _ = input_file_writer.write_all(key.as_bytes());
            let _ = input_file_writer.write_all("\n".as_bytes());

            file_lines += 1;

            if file_lines >= file_max_lines {
                input_file_writer.flush()?;
                file_num += 1;
                let mut file_subfix = "".to_string();
                file_subfix.push_str(&file_num.to_string());
                let input_file_name =
                    gen_file_path(meta_dir, OBJECT_LIST_FILE_PREFIX, &file_subfix);
                drop(input_file_writer);

                let input_file_builder = FileOperationBuilder::new(&input_file_name)
                    .with_append_mode()
                    .with_parent_creation();
                input_file_writer = input_file_builder.build_line_writer().context(format!(
                    "{}:{}",
                    file!(),
                    line!()
                ))?;

                // 记录新的文件列表
                let _ = sequence_line_writer.write_all(input_file_name.clone().as_bytes());
                let _ = sequence_line_writer.write_all("\n".as_bytes());
                file_lines = 0;
            }
        };
    }

    input_file_writer
        .flush()
        .context(format!("{}:{}", file!(), line!()))?;
    let desc = list_files_desc_from_sequence_file(&sequence_file_name)?;
    Ok(desc)
}

/// 向文件追加一行内容
///
/// # 参数
/// * `file_name` - 文件路径
/// * `content` - 要追加的内容
///
/// # 返回值
/// 追加成功返回 Ok(())，失败返回错误
pub fn append_line_to_file(file_name: &str, content: &str) -> Result<()> {
    let append_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)?;
    let mut append_linewiter = LineWriter::new(&append_file);
    append_linewiter.write_all(content.as_bytes())?;
    append_linewiter.write_all("\n".as_bytes())?;
    Ok(())
}

// 生成指定字节数的文件
/// 生成指定大小的随机内容文件
///
/// # 参数
/// * `file_size` - 文件大小（字节）
/// * `chunk_size` - 分块大小
/// * `file_name` - 文件路径
///
/// # 返回值
/// 生成成功返回 Ok(())，失败返回错误
pub fn generate_file(file_size: usize, chunk_size: usize, file_name: &str) -> Result<()> {
    let str_len = file_size / chunk_size;
    let remainder = file_size % chunk_size;

    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_name)?;
    let mut file = LineWriter::new(file_ref);
    let str = rand_string(chunk_size);
    for _ in 0..str_len {
        let _ = file.write_all(str.as_bytes());
    }

    if remainder > 0 {
        let str = rand_string(remainder);
        let _ = file.write_all(str.as_bytes());
    }

    file.flush()?;

    Ok(())
}

// 用 0 填充临时文件
/// 用零字节填充文件到指定大小
///
/// # 参数
/// * `file_size` - 文件大小（字节）
/// * `chunk_size` - 分块大小
/// * `file_name` - 文件路径
///
/// # 返回值
/// 填充成功返回 Ok(())，失败返回错误
pub fn fill_file_with_zero(file_size: usize, chunk_size: usize, file_name: &str) -> Result<()> {
    let batch = file_size / chunk_size;
    let remainder = file_size % chunk_size;

    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let mut file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_name)?;
    // let mut file = LineWriter::new(file_ref);
    let buffer = vec![0; chunk_size];
    for _ in 0..batch {
        let _ = file_ref.write_all(&buffer);
    }

    if remainder > 0 {
        let buffer = vec![0; remainder];
        let _ = file_ref.write_all(&buffer);
    }

    file_ref.flush()?;

    Ok(())
}

/// 将一个文件的内容合并到另一个文件中
///
/// # 参数
/// * `file` - 源文件路径
/// * `merge_to` - 目标文件路径
/// * `chunk_size` - 分块大小
///
/// # 返回值
/// 合并成功返回 Ok(())，失败返回错误
pub fn merge_file<P: AsRef<Path>>(file: P, merge_to: P, chunk_size: usize) -> Result<()> {
    let mut f = OpenOptions::new().read(true).open(file)?;
    let mut merge_to = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(merge_to)?;

    loop {
        let mut buffer = vec![0; chunk_size];
        let read_count = f.read(&mut buffer)?;
        let buf = &buffer[..read_count];
        merge_to.write_all(&buf)?;
        if read_count != chunk_size {
            break;
        }
    }
    merge_to.flush()?;
    Ok(())
}

// 生成指定行数，指定每行字节数的文件
#[allow(dead_code)]
/// 生成指定行数和每行字节数的文件
///
/// # 参数
/// * `line_base_size` - 每行的基础字节数
/// * `lines` - 总行数
/// * `file_name` - 文件路径
///
/// # 返回值
/// 生成成功返回 Ok(())，失败返回错误
pub fn generate_line_file(line_base_size: usize, lines: usize, file_name: &str) -> Result<()> {
    // 生成文件目录
    let store_path = Path::new(file_name);
    let path = std::path::Path::new(store_path);
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    };

    let file_ref = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_name)?;
    let mut file = LineWriter::new(file_ref);
    let str = rand_string(line_base_size);
    for i in 0..lines {
        let mut line = str.clone();
        line.push_str(&i.to_string());
        line.push_str("\n");
        let _ = file.write_all(line.as_bytes());
    }
    file.flush()?;

    Ok(())
}

// 生成文件的函数，具体功能取决于函数内部实现。
// 该函数对外公开，可以在其他模块中调用。
/// 在指定目录中生成多个随机文件
///
/// # 参数
/// * `dir` - 目标目录路径
/// * `file_prefix_len` - 文件名前缀长度
/// * `file_size` - 每个文件的大小
/// * `chunk_size` - 分块大小
/// * `file_quantity` - 文件数量
///
/// # 返回值
/// 生成成功返回 Ok(())，失败返回错误
pub fn generate_files(
    dir: &str,
    file_prefix_len: usize,
    file_size: usize,
    chunk_size: usize,
    file_quantity: usize,
) -> Result<()> {
    let dir_path = Path::new(dir);
    // 检查目录是否存在，如果不存在则创建
    if !dir_path.exists() {
        std::fs::create_dir_all(dir_path)?;
    };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()?;

    let batch = file_size / chunk_size;
    let remainder = file_size % chunk_size;
    let chunk = rand_string(chunk_size);
    let last_chunk = match remainder > 0 {
        true => rand_string(remainder),
        false => "".to_string(),
    };

    pool.scope(|s| {
        for _ in 0..file_quantity {
            let mut ck = chunk.clone();
            let mut l_ck = last_chunk.clone();
            s.spawn(move |_| {
                let mut file_prefix = rand_path(file_prefix_len);
                let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
                file_prefix.push_str(now.to_string().as_str());

                let file_path = match dir.ends_with("/") {
                    true => {
                        let mut file_path = dir.to_string();
                        file_path.push_str(file_prefix.as_str());
                        file_path
                    }
                    false => {
                        let mut file_path = dir.to_string();
                        file_path.push_str("/");
                        file_path.push_str(file_prefix.as_str());
                        file_path
                    }
                };

                let mut file = match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(file_path)
                {
                    Ok(f) => f,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                };

                for _ in 0..batch {
                    let changed_char = rand_string(1);
                    ck.remove(0);
                    ck.insert(ck.len() - 1, changed_char.chars().next().unwrap());
                    let _ = file.write_all(ck.as_bytes());
                }

                if remainder > 0 {
                    let changed_char = rand_string(1);
                    l_ck.remove(0);
                    l_ck.insert(l_ck.len() - 1, changed_char.chars().next().unwrap());
                    let _ = file.write_all(l_ck.as_bytes());
                }

                if let Err(e) = file.flush() {
                    log::error!("{:?}", e);
                };
            });
        }
    });

    Ok(())
}

/// 根据 chunk_size 拆分文件，生成文件块列表
/// 根据分块大小生成文件分片计划
///
/// # 参数
/// * `file_path` - 文件路径
/// * `chunk_size` - 分块大小
///
/// # 返回值
/// 返回文件分片列表，包含每个分片的编号和偏移量
pub fn gen_file_part_plan(file_path: &str, chunk_size: usize) -> Result<Vec<FilePart>> {
    let mut vec_file_parts: Vec<FilePart> = vec![];
    let f = File::open(file_path).context(format!("{}:{}", file!(), line!()))?;
    let meta = f.metadata().context(format!("{}:{}", file!(), line!()))?;
    let file_len = meta.len();
    let chunk_size_u64 =
        TryInto::<u64>::try_into(chunk_size).context(format!("{}:{}", file!(), line!()))?;
    let mut offset = 0;
    let quotient = file_len / chunk_size_u64;
    let remainder = file_len % chunk_size_u64;
    let part_quantities = match remainder.eq(&0) {
        true => quotient,
        false => quotient + 1,
    };
    for part_num_u64 in 1..=part_quantities {
        let part_num = TryInto::<i32>::try_into(part_num_u64)?;
        let file_part = FilePart { part_num, offset };
        vec_file_parts.push(file_part);
        offset += chunk_size_u64;
    }

    Ok(vec_file_parts)
}

/// 创建文件路径的父目录
///
/// # 参数
/// * `file_path` - 文件路径
///
/// # 返回值
/// 创建成功返回 Ok(())，失败返回错误
pub fn create_parent_dir<P: AsRef<Path>>(file_path: P) -> Result<()> {
    // if let Some(p) = Path::new(file_path).parent() {
    if let Some(p) = file_path.as_ref().parent() {
        std::fs::create_dir_all(p).context(format!("{}:{}", file!(), line!()))?;
    };
    Ok(())
}

/// 计算文件的字节数
///
/// # 参数
/// * `filename` - 文件路径
///
/// # 返回值
/// 返回文件的字节数，失败返回错误
pub fn count_file_bytes<P: AsRef<Path>>(filename: P) -> Result<u64> {
    let metadata = fs::metadata(filename).context(format!("{}:{}", file!(), line!()))?;
    Ok(metadata.len())
}

/// 计算文件的行数
///
/// # 参数
/// * `filename` - 文件路径
///
/// # 返回值
/// 返回文件的行数，失败返回错误
pub fn count_file_lines<P: AsRef<Path>>(filename: P) -> Result<u64> {
    let file = File::open(filename).context(format!("{}:{}", file!(), line!()))?;
    let reader = io::BufReader::new(file);
    Ok(reader.lines().count() as u64)
}

/// 根据列表序列文件，生成所有列表文件的 FileDescription
///
/// # 参数
/// * `sequence_file` - 序列文件路径
///
/// # 返回值
/// 返回文件描述列表，包含每个文件的路径、大小和行数
pub fn list_files_desc_from_sequence_file<P: AsRef<Path>>(
    sequence_file: P,
) -> Result<Vec<FileDescription>> {
    let mut file_desc_vec = vec![];
    let lines = read_lines(sequence_file).context(format!("{}:{}", file!(), line!()))?;
    for line in lines {
        let path = line.context(format!("{}:{}", file!(), line!()))?;
        let size = count_file_bytes(path.as_str())?;
        let total_lines =
            count_file_lines(path.as_str()).context(format!("{}:{}", file!(), line!()))?;

        let desc = FileDescription {
            path,
            size,
            total_lines,
        };
        file_desc_vec.push(desc);
    }

    Ok(file_desc_vec)
}

/// 根据文件路径列表生成对应的文件描述
///
/// # 参数
/// * `files` - 文件路径列表
///
/// # 返回值
/// 返回文件描述列表，包含每个文件的路径、大小和行数
pub fn files_description(files: Vec<String>) -> Result<Vec<FileDescription>> {
    let mut file_desc_vec = vec![];
    for path in files {
        let size = count_file_bytes(path.as_str()).context(format!("{}:{}", file!(), line!()))?;
        let total_lines =
            count_file_lines(path.as_str()).context(format!("{}:{}", file!(), line!()))?;

        let desc = FileDescription {
            path,
            size,
            total_lines,
        };
        file_desc_vec.push(desc);
    }

    Ok(file_desc_vec)
}

#[cfg(test)]
mod test {
    use std::time::Instant;

    use crate::commons::{
        analyze_folder_files_size_parallel, fileutiles::generate_file, fill_file_with_zero,
        multi_parts_copy_file,
    };

    use super::generate_line_file;

    //cargo test commons::fileutiles::test::test_generate_line_file -- --nocapture
    #[test]
    fn test_generate_line_file() {
        let r = generate_line_file(1020, 1048576, "/tmp/gen/gen_line_file");
        println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_gen_file -- --nocapture
    #[test]
    fn test_gen_file() {
        let _ = generate_file(128, 8, "/tmp/gen/gen_file");
        // println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_multi_parts_copy_file -- --nocapture
    #[test]
    fn test_multi_parts_copy_file() {
        let r = multi_parts_copy_file("/tmp/oss_pipe", "/tmp/genfilecp", 1024);
        println!("test scan result {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_fill_file_with_zero -- --nocapture
    #[test]
    fn test_fill_file_with_zero() {
        let r = fill_file_with_zero(1024 * 1024 * 1024 * 10, 1024 * 1024, "/tmp/zero_file");
        println!("test_fill_file_with_zero {:?}", r);
    }

    //cargo test commons::fileutiles::test::test_analyze_folder_files_size_parallel -- --nocapture
    #[tokio::test]
    async fn test_analyze_folder_files_size_parallel() {
        let now = Instant::now();
        let r = analyze_folder_files_size_parallel("/root/files", None, None, 64, 4)
            .await
            .unwrap();

        println!("analyze:{:?}", r);
        let total = r.iter().map(|i| *i.value()).sum::<i128>();
        println!("{}", total);

        println!("elapsed:{:?}", now.elapsed());
    }
}
