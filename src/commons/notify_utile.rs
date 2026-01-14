use super::struct_to_json_string;
use crate::{
    commons::FileOperationBuilder,
    consts::task_consts::{NOTIFY_FILES_PREFIX, NOTIFY_SEQUENCE_FILE},
    tasks::gen_file_path,
};
use anyhow::{Context, Result};
use notify::{
    event::CreateKind, Config, Error, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{LineWriter, Write},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::Receiver,
        Arc,
    },
};
use tokio::task::yield_now;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PathType {
    Folder,
    File,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ModifyType {
    Unkown,
    Create,
    Delete,
    Modify,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct Modified {
    pub path: String,
    pub path_type: PathType,
    pub modify_type: ModifyType,
}

impl Modified {
    pub fn new() -> Self {
        Self {
            path: "".to_string(),
            path_type: PathType::File,
            modify_type: ModifyType::Unkown,
        }
    }
}

#[derive(Debug)]
pub struct NotifyWatcher {
    watcher: RecommendedWatcher,
    reciver: Receiver<Result<Event, Error>>,
    writing_file_status: bool,
    pub watched_dir: String,
}

// Todo
// 新增 write_stop 用来替换 notify 文件
impl NotifyWatcher {
    // pub fn new<P: AsRef<Path>>(watched_dir: P) -> notify::Result<Self> {
    pub fn new<P: AsRef<Path>>(watched_dir: P) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, Config::default())?;
        watcher.watch(watched_dir.as_ref(), RecursiveMode::Recursive)?;

        match watched_dir.as_ref().to_str() {
            Some(watched_dir) => Ok(Self {
                watcher,
                watched_dir: watched_dir.to_string(),
                reciver: rx,
                writing_file_status: false,
            }),
            None => {
                return Err(anyhow::anyhow!("watched dir not exists"));
            }
        }

        // Ok(Self {
        //     // watcher,
        //     watched_dir: watched_dir.as_ref().to_str().unwrap().to_string(),
        //     reciver: rx,
        //     writing_file_status: false,
        // })
    }

    // Todo
    // 分文件保存追加记录，消费完成删除，避免文件过大
    pub async fn watch_to_file(
        mut self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        file: File,
        file_size: Arc<AtomicU64>,
    ) {
        let mut linewiter = LineWriter::new(&file);

        self.writing_file_status = true;
        for res in self.reciver {
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst)
                || err_occur.load(std::sync::atomic::Ordering::SeqCst)
            {
                return;
            }

            if !self.writing_file_status {
                return;
            }
            let mut modified = Modified::new();
            match res {
                Ok(event) => {
                    modified.path = event.paths[0].as_path().display().to_string();
                    match event.kind {
                        EventKind::Create(c) => match c {
                            CreateKind::File => {
                                modified.modify_type = ModifyType::Create;
                                modified.path_type = PathType::File;
                            }
                            CreateKind::Folder => {
                                modified.modify_type = ModifyType::Create;
                                modified.path_type = PathType::Folder;
                            }
                            CreateKind::Any => {}
                            CreateKind::Other => {}
                        },
                        EventKind::Modify(m) => match m {
                            notify::event::ModifyKind::Data(_) => {
                                modified.modify_type = ModifyType::Modify;
                                modified.path_type = PathType::File;
                            }
                            notify::event::ModifyKind::Any => {}
                            notify::event::ModifyKind::Metadata(_) => {}
                            notify::event::ModifyKind::Name(_) => {}
                            notify::event::ModifyKind::Other => {}
                        },
                        EventKind::Remove(r) => match r {
                            notify::event::RemoveKind::File => {
                                modified.modify_type = ModifyType::Delete;
                                modified.path_type = PathType::File;
                            }
                            notify::event::RemoveKind::Folder => {
                                modified.modify_type = ModifyType::Delete;
                                modified.path_type = PathType::Folder;
                            }
                            notify::event::RemoveKind::Any => {}
                            notify::event::RemoveKind::Other => {}
                        },
                        _ => {}
                    }
                }
                Err(error) => println!("{}", error),
            }

            match modified.modify_type {
                ModifyType::Unkown => {}
                _ => {
                    match struct_to_json_string(&modified) {
                        Ok(json) => {
                            let _ = linewiter.write_all(json.as_bytes());
                            let _ = linewiter.write_all("\n".as_bytes());
                        }
                        Err(e) => {
                            log::error!("{:?}", e)
                        }
                    };
                }
            }

            match file.metadata() {
                Ok(meta) => {
                    file_size.store(meta.len(), Ordering::SeqCst);
                }
                Err(_) => {}
            };
            yield_now().await;
        }
    }

    //Todo 变更为多文件模式
    pub async fn watch_to_files(
        mut self,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
        meta_dir: &str,
        file_max_lines: usize,
    ) -> Result<()> {
        let mut lines_counter = 0;
        let mut notify_num = 0;
        let meta_dir_path = Path::new(meta_dir);
        let notify_sequence = meta_dir_path.join(NOTIFY_SEQUENCE_FILE);
        let notify_sequence_builder =
            FileOperationBuilder::new(notify_sequence).with_parent_creation();

        let notify_name = NOTIFY_FILES_PREFIX.to_string() + &notify_num.to_string();
        let mut notify_path = meta_dir_path.join(notify_name);
        let notify_input_builder = FileOperationBuilder::new(&notify_path).with_parent_creation();

        let mut notify_sequence_line_writer = notify_sequence_builder
            .build_line_writer()
            .context(format!("{}:{}", file!(), line!()))?;
        let mut notify_file_writer =
            notify_input_builder
                .build_line_writer()
                .context(format!("{}:{}", file!(), line!()))?;

        notify_sequence_line_writer
            .write_all(&notify_path.as_path().display().to_string().as_bytes())?;
        notify_sequence_line_writer.write_all("\n".as_bytes())?;

        self.writing_file_status = true;
        for res in self.reciver {
            if stop_mark.load(std::sync::atomic::Ordering::SeqCst)
                || err_occur.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(anyhow::anyhow!("watch_to_files stop"));
            }

            if !self.writing_file_status {
                return Err(anyhow::anyhow!("watch_to_files not writing"));
            }
            let mut modified = Modified::new();

            let event = res?;

            match event.kind {
                EventKind::Create(c) => match c {
                    CreateKind::File => {
                        modified.modify_type = ModifyType::Create;
                        modified.path_type = PathType::File;
                    }
                    CreateKind::Folder => {
                        modified.modify_type = ModifyType::Create;
                        modified.path_type = PathType::Folder;
                    }
                    CreateKind::Any => {}
                    CreateKind::Other => {}
                },
                EventKind::Modify(m) => match m {
                    notify::event::ModifyKind::Data(_) => {
                        modified.modify_type = ModifyType::Modify;
                        modified.path_type = PathType::File;
                    }
                    notify::event::ModifyKind::Any => {}
                    notify::event::ModifyKind::Metadata(_) => {}
                    notify::event::ModifyKind::Name(_) => {}
                    notify::event::ModifyKind::Other => {}
                },
                EventKind::Remove(r) => match r {
                    notify::event::RemoveKind::File => {
                        modified.modify_type = ModifyType::Delete;
                        modified.path_type = PathType::File;
                    }
                    notify::event::RemoveKind::Folder => {
                        modified.modify_type = ModifyType::Delete;
                        modified.path_type = PathType::Folder;
                    }
                    notify::event::RemoveKind::Any => {}
                    notify::event::RemoveKind::Other => {}
                },
                _ => {}
            }

            match modified.modify_type {
                ModifyType::Unkown => {}
                _ => {
                    match struct_to_json_string(&modified) {
                        Ok(json) => {
                            let _ = notify_file_writer.write_all(json.as_bytes());
                            let _ = notify_file_writer.write_all("\n".as_bytes());
                            if lines_counter >= file_max_lines {
                                lines_counter = 0;
                                notify_num += 1;
                                notify_path = meta_dir_path.join(
                                    NOTIFY_FILES_PREFIX.to_string() + &notify_num.to_string(),
                                );
                                let notify_input_builder =
                                    FileOperationBuilder::new(&notify_path).with_parent_creation();
                                notify_file_writer = notify_input_builder
                                    .build_line_writer()
                                    .context(format!("{}:{}", file!(), line!()))?;
                                notify_sequence_line_writer.write_all(
                                    &notify_path.as_path().display().to_string().as_bytes(),
                                )?;
                                notify_sequence_line_writer.write_all("\n".as_bytes())?;
                            } else {
                                lines_counter += 1;
                            }
                        }
                        Err(e) => {
                            log::error!("{:?}", e)
                        }
                    };
                }
            }

            yield_now().await;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn stop_write_file(&mut self) -> Result<()> {
        self.writing_file_status = false;
        let tmp_file = gen_file_path(self.watched_dir.as_str(), "oss_pipe_tmp", "");
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_file.as_str())?;
        drop(f);
        fs::remove_dir(tmp_file)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn writing_file_status(&self) -> bool {
        self.writing_file_status
    }
}

#[derive(Debug)]
pub struct NotifyFolder(
    RecommendedWatcher,
    pub Receiver<Result<Event, Error>>,
    pub Arc<AtomicBool>,
    pub Arc<AtomicBool>,
    pub String,
);

impl NotifyFolder {
    pub fn new<P: AsRef<Path>>(
        watched_dir: P,
        stop_mark: Arc<AtomicBool>,
        err_occur: Arc<AtomicBool>,
    ) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, Config::default())?;
        watcher.watch(watched_dir.as_ref(), RecursiveMode::Recursive)?;

        match watched_dir.as_ref().to_str() {
            Some(watched_dir) => Ok(Self(
                watcher,
                rx,
                stop_mark,
                err_occur,
                watched_dir.to_string(),
            )),
            None => {
                return Err(anyhow::anyhow!("watched dir not exists"));
            }
        }
    }

    pub async fn watch_to_files(mut self, meta_dir: &str, file_max_lines: usize) -> Result<()> {
        self.3.store(false, std::sync::atomic::Ordering::SeqCst);
        self.2.store(false, std::sync::atomic::Ordering::SeqCst);
        let mut lines_counter = 0;
        let mut notify_num = 0;
        let meta_dir_path = Path::new(meta_dir);
        let notify_sequence = meta_dir_path.join(NOTIFY_SEQUENCE_FILE);
        let notify_sequence_builder =
            FileOperationBuilder::new(notify_sequence).with_parent_creation();

        let notify_name = NOTIFY_FILES_PREFIX.to_string() + &notify_num.to_string();
        let mut notify_path = meta_dir_path.join(notify_name);
        let notify_input_builder = FileOperationBuilder::new(&notify_path).with_parent_creation();

        let mut notify_sequence_line_writer = notify_sequence_builder
            .build_line_writer()
            .context(format!("{}:{}", file!(), line!()))?;
        let mut notify_file_writer =
            notify_input_builder
                .build_line_writer()
                .context(format!("{}:{}", file!(), line!()))?;

        notify_sequence_line_writer
            .write_all(&notify_path.as_path().display().to_string().as_bytes())?;
        notify_sequence_line_writer.write_all("\n".as_bytes())?;

        for res in self.1 {
            if self.3.load(std::sync::atomic::Ordering::SeqCst)
                || self.2.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(());
            }

            let mut modified = Modified::new();

            let event = res?;

            match event.kind {
                EventKind::Create(c) => match c {
                    CreateKind::File => {
                        modified.modify_type = ModifyType::Create;
                        modified.path_type = PathType::File;
                    }
                    CreateKind::Folder => {
                        modified.modify_type = ModifyType::Create;
                        modified.path_type = PathType::Folder;
                    }
                    CreateKind::Any => {}
                    CreateKind::Other => {}
                },
                EventKind::Modify(m) => match m {
                    notify::event::ModifyKind::Data(_) => {
                        modified.modify_type = ModifyType::Modify;
                        modified.path_type = PathType::File;
                    }
                    notify::event::ModifyKind::Any => {}
                    notify::event::ModifyKind::Metadata(_) => {}
                    notify::event::ModifyKind::Name(_) => {}
                    notify::event::ModifyKind::Other => {}
                },
                EventKind::Remove(r) => match r {
                    notify::event::RemoveKind::File => {
                        modified.modify_type = ModifyType::Delete;
                        modified.path_type = PathType::File;
                    }
                    notify::event::RemoveKind::Folder => {
                        modified.modify_type = ModifyType::Delete;
                        modified.path_type = PathType::Folder;
                    }
                    notify::event::RemoveKind::Any => {}
                    notify::event::RemoveKind::Other => {}
                },
                _ => {}
            }

            match modified.modify_type {
                ModifyType::Unkown => {}
                _ => {
                    match struct_to_json_string(&modified) {
                        Ok(json) => {
                            let _ = notify_file_writer.write_all(json.as_bytes());
                            let _ = notify_file_writer.write_all("\n".as_bytes());
                            if lines_counter >= file_max_lines - 1 {
                                lines_counter = 0;
                                notify_num += 1;
                                notify_path = meta_dir_path.join(
                                    NOTIFY_FILES_PREFIX.to_string() + &notify_num.to_string(),
                                );
                                let notify_input_builder =
                                    FileOperationBuilder::new(&notify_path).with_parent_creation();
                                notify_file_writer = notify_input_builder
                                    .build_line_writer()
                                    .context(format!("{}:{}", file!(), line!()))?;
                                notify_sequence_line_writer.write_all(
                                    &notify_path.as_path().display().to_string().as_bytes(),
                                )?;
                                notify_sequence_line_writer.write_all("\n".as_bytes())?;
                            } else {
                                lines_counter += 1;
                            }
                        }
                        Err(e) => {
                            log::error!("{:?}", e)
                        }
                    };
                }
            }
            yield_now().await;
        }
        Ok(())
    }

    pub fn stop_watch(&mut self) {
        self.2.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::{self, OpenOptions},
        sync::{
            atomic::{AtomicBool, AtomicU64},
            Arc,
        },
    };

    use crate::tasks::gen_file_path;

    use super::NotifyWatcher;
    use tokio::{
        runtime,
        task::{self, JoinSet},
    };

    //cargo test commons::notify_utile::test::test_watch_to_files -- --nocapture
    // #[test]
    #[tokio::test]
    async fn test_watch_to_files() {
        // let rt = runtime::Builder::new_multi_thread().build().eanble.unwrap();
        // let mut set: JoinSet<()> = JoinSet::new();

        let stop_mark = Arc::new(AtomicBool::new(false));
        let err_occur = Arc::new(AtomicBool::new(false));
        let s = stop_mark.clone();
        let e = err_occur.clone();
        // let _rt_rs = rt.block_on(async move {
        // set.spawn(async move {
        println!("begin watch");
        let notify_watcher = NotifyWatcher::new("/root/files").unwrap();
        if let Err(e) = notify_watcher.watch_to_files(s, e, "/tmp", 10).await {
            eprint!("{}", e);
        }
    }

    //cargo test commons::notify_utile::test::test_watcher -- --nocapture
    #[test]
    fn test_watcher() {
        let watch_file_name = "/tmp/watch.log";
        let rt = runtime::Builder::new_current_thread().build().unwrap();

        let mut set: JoinSet<()> = JoinSet::new();
        let file_size = Arc::new(AtomicU64::new(0));
        let _rt_rs = rt.block_on(async move {
            let _rs_watch = set.spawn(async move {
                println!("begin watch");
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(watch_file_name)
                    .unwrap();
                let notify_watcher = NotifyWatcher::new("/tmp/files").unwrap();
                notify_watcher
                    .watch_to_file(
                        Arc::new(AtomicBool::new(false)),
                        Arc::new(AtomicBool::new(false)),
                        file,
                        Arc::clone(&file_size),
                    )
                    .await;
            });
            let _rs_read = set.spawn(async move {
                println!("begin read watch file");
            });
            while set.len() > 0 {
                set.join_next().await;
            }
        });
    }
}
