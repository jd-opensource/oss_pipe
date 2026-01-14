use crate::cmd::cmd_gen_file::{new_gen_file_cmd, new_gen_files_cmd};
use crate::cmd::cmd_task::new_task_cmd;
use crate::cmd::{
    new_command_tree_cmd, new_config_cmd, new_exit_cmd, new_parameters_cmd, new_template,
};
use crate::commons::yamlutile::struct_to_yml_file;
use crate::commons::{
    byte_size_str_to_usize, generate_file, generate_files, read_yaml_file, SubCmd,
};
use crate::commons::{struct_to_yaml_string, CommandCompleter};
use crate::configure::{generate_default_config, get_config, set_config_file_path};
use crate::configure::{get_config_file_path, get_current_config_yml, set_config};
use crate::consts::cmd_consts::APP_NAME;
use crate::interact::INTERACT_STATUS;
use crate::models::model_filters::{LastModifyFilter, LastModifyFilterType};
use crate::models::model_s3::{OSSDescription, OssProvider};
use crate::models::model_task::{ObjectStorage, Task, TaskType};
use crate::models::model_task_compare::CompareTask;
use crate::models::model_task_delete_bucket::TaskDeleteBucket;
use crate::models::model_task_transfer::{IncrementMode, TransferTask};
use crate::{interact, tracing_init};
use clap::{Arg, ArgAction, ArgMatches, Command as Clap_Command};
use lazy_static::lazy_static;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::runtime;

lazy_static! {
    static ref CLIAPP: Clap_Command = Clap_Command::new(APP_NAME)
        .version("1.0")
        .author("Shiwen Jia. <jiashiwen@gmail.com>")
        .about("oss_pipe")
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
        )
        .arg(
            Arg::new("interact")
                .short('i')
                .long("interact")
                .action(ArgAction::SetTrue)
                .help("run as interact mod")
        )
        .subcommand(new_task_cmd())
        .subcommand(new_template())
        .subcommand(new_parameters_cmd())
        .subcommand(new_config_cmd())
        .subcommand(new_gen_file_cmd())
        .subcommand(new_gen_files_cmd())
        .subcommand(new_exit_cmd())
        .subcommand(new_command_tree_cmd());
    static ref SUBCMDS: Vec<SubCmd> = subcommands();
}

pub fn run_app() {
    set_config("").unwrap();
    let matches = CLIAPP.clone().get_matches();
    cmd_match(&matches);
}

pub fn run_from(args: Vec<String>) {
    match Clap_Command::try_get_matches_from(CLIAPP.to_owned(), args.clone()) {
        Ok(matches) => {
            cmd_match(&matches);
        }
        Err(err) => {
            err.print().expect("Error writing Error");
        }
    };
}

// 获取全部子命令，用于构建 commandcompleter
pub fn all_subcommand(app: &Clap_Command, beginlevel: usize, input: &mut Vec<SubCmd>) {
    let nextlevel = beginlevel + 1;
    let mut subcmds = vec![];
    for iterm in app.get_subcommands() {
        subcmds.push(iterm.get_name().to_string());
        if iterm.has_subcommands() {
            all_subcommand(iterm, nextlevel, input);
        } else {
            if beginlevel == 0 {
                all_subcommand(iterm, nextlevel, input);
            }
        }
    }
    let subcommand = SubCmd {
        level: beginlevel,
        command_name: app.get_name().to_string(),
        subcommands: subcmds,
    };
    input.push(subcommand);
}

pub fn get_cmd_tree(cmd: &Clap_Command) -> termtree::Tree<String> {
    let mut tree = termtree::Tree::new(cmd.get_name().to_string());
    if cmd.has_subcommands() {
        let mut vec_t = vec![];
        for item in cmd.get_subcommands() {
            let t = get_cmd_tree(item);
            vec_t.push(t);
        }
        tree = tree.with_leaves(vec_t);
    }
    tree
}

pub fn get_command_completer() -> CommandCompleter {
    CommandCompleter::new(SUBCMDS.to_vec())
}

fn subcommands() -> Vec<SubCmd> {
    let mut subcmds = vec![];
    all_subcommand(&CLIAPP, 0, &mut subcmds);
    subcmds
}

fn cmd_match(matches: &ArgMatches) {
    if let Some(c) = matches.get_one::<String>("config") {
        set_config_file_path(c.to_string()).unwrap();
        set_config(&get_config_file_path()).unwrap();
    } else {
        set_config("").unwrap();
    }

    if !INTERACT_STATUS.load(std::sync::atomic::Ordering::SeqCst) {
        let log_level = get_config().unwrap().log_level;
        tracing_init(&log_level);
    }

    if matches.get_flag("interact") {
        if !INTERACT_STATUS.load(std::sync::atomic::Ordering::SeqCst) {
            interact::run();
            return;
        }
    }

    if let Some(config) = matches.subcommand_matches("config") {
        if let Some(_show) = config.subcommand_matches("show") {
            let yml = get_current_config_yml();
            match yml {
                Ok(str) => {
                    println!("{}", str);
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        if let Some(gen_config) = config.subcommand_matches("gendefault") {
            let mut file = String::from("");
            if let Some(path) = gen_config.get_one::<String>("filepath") {
                file.push_str(path);
            } else {
                file.push_str("config_default.yml")
            }
            if let Err(e) = generate_default_config(file.as_str()) {
                log::error!("{:?}", e);
                return;
            };
            println!("{} created!", file);
        }
    }

    if let Some(task) = matches.subcommand_matches("task") {
        let rt = match runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .enable_all()
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };

        if let Some(analyze) = task.subcommand_matches("analyze") {
            if let Some(f) = analyze.get_one::<String>("filepath") {
                let task = match read_yaml_file::<Task>(f) {
                    Ok(t) => t,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                };

                match task {
                    Task::Transfer(t) => {
                        let _ = rt.block_on(async {
                            if let Err(e) = t.analyze().await {
                                log::error!("{:?}", e);
                                return;
                            }
                        });
                    }
                    _ => {
                        println!("No analysis required");
                    }
                };
            }
        }

        if let Some(exec) = task.subcommand_matches("exec") {
            if let Some(f) = exec.get_one::<String>("filepath") {
                let task = read_yaml_file::<Task>(f);

                match task {
                    Ok(t) => {
                        let _ = rt.block_on(async { t.execute().await });
                    }
                    Err(e) => {
                        log::error!("{:#?}", e);
                    }
                }
            }
        }

        //Todo 待改造，支持指定目录参数
        if let Some(list_objects) = task.subcommand_matches("list_objects") {
            let task_file = match list_objects.get_one::<String>("taskfile") {
                Some(f) => f,
                None => {
                    return;
                }
            };

            let folder = match list_objects.get_one::<String>("list_files_folder") {
                Some(f) => f,
                None => {
                    return;
                }
            };

            let task = match read_yaml_file::<Task>(task_file) {
                Ok(t) => t,
                Err(e) => {
                    log::error!("{:?}", e);
                    return;
                }
            };

            match task {
                Task::Transfer(t) => rt.block_on(async {
                    if let Err(e) = t.list_objects_to_files(folder).await {
                        log::error!("{:?}", e);
                        return;
                    }
                }),
                _ => {
                    println!("No objects list");
                }
            };
        }
    }

    if let Some(template) = matches.subcommand_matches("template") {
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n,
            Err(e) => {
                log::error!("{:?}", e);
                return;
            }
        };

        if let Some(transfer) = template.subcommand_matches("transfer") {
            if let Some(oss2oss) = transfer.subcommand_matches("oss2oss") {
                let now = time::OffsetDateTime::now_utc().unix_timestamp();
                let file = oss2oss.get_one::<String>("file");
                let mut transfer_oss2oss = TransferTask::default();
                transfer_oss2oss.name = "transfer_oss2oss".to_string();
                transfer_oss2oss.attributes.task_parallelism = num_cpus::get();

                let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
                let exclude_vec = vec![
                    r"\b[\w-]*(https?|ftp|file):\/\/\S+".to_string(),
                    "test/t4/*".to_string(),
                ];
                transfer_oss2oss.attributes.exclude = Some(exclude_vec);
                transfer_oss2oss.attributes.include = Some(include_vec);
                let mut oss_desc = OSSDescription::default();
                oss_desc.provider = OssProvider::ALI;
                oss_desc.endpoint = "http://oss-cn-beijing.aliyuncs.com".to_string();
                transfer_oss2oss.source = ObjectStorage::OSS(oss_desc);
                transfer_oss2oss.attributes.last_modify_filter = Some(LastModifyFilter {
                    filter_type: LastModifyFilterType::Greater,
                    timestamp: usize::try_from(now).unwrap(),
                });
                transfer_oss2oss.attributes.increment_mode = IncrementMode::Scan { interval: 3600 };

                let task = Task::Transfer(transfer_oss2oss);

                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }
            if let Some(oss2local) = transfer.subcommand_matches("oss2local") {
                let file = oss2local.get_one::<String>("file");
                let mut transfer_oss2local = TransferTask::default();
                transfer_oss2local.name = "transfer_oss2local".to_string();
                let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
                let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
                transfer_oss2local.attributes.exclude = Some(exclude_vec);
                transfer_oss2local.attributes.include = Some(include_vec);
                let target: &str = "/tmp";
                transfer_oss2local.target = ObjectStorage::Local(target.to_string());
                transfer_oss2local.attributes.last_modify_filter = Some(LastModifyFilter {
                    filter_type: LastModifyFilterType::Greater,
                    timestamp: usize::try_from(now.as_secs()).unwrap(),
                });

                let task = Task::Transfer(transfer_oss2local);

                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }
            if let Some(local2oss) = transfer.subcommand_matches("local2oss") {
                let file = local2oss.get_one::<String>("file");
                let mut transfer_local2oss = TransferTask::default();
                transfer_local2oss.name = "transfer_local2oss".to_string();
                transfer_local2oss.attributes.task_parallelism = num_cpus::get();
                transfer_local2oss.attributes.multi_part_parallelism = num_cpus::get() * 2;
                let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
                let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
                transfer_local2oss.attributes.exclude = Some(exclude_vec);
                transfer_local2oss.attributes.include = Some(include_vec);
                let source: &str = "/tmp";
                transfer_local2oss.source = ObjectStorage::Local(source.to_string());
                transfer_local2oss.attributes.last_modify_filter = Some(LastModifyFilter {
                    filter_type: LastModifyFilterType::Greater,
                    timestamp: usize::try_from(now.as_secs()).unwrap(),
                });

                let task = Task::Transfer(transfer_local2oss);

                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }

            if let Some(local2local) = transfer.subcommand_matches("local2local") {
                let file = local2local.get_one::<String>("file");
                let mut transfer_local2local = TransferTask::default();
                transfer_local2local.name = "transfer_local2local".to_string();
                transfer_local2local.attributes.task_parallelism = num_cpus::get();
                let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
                let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
                transfer_local2local.attributes.exclude = Some(exclude_vec);
                transfer_local2local.attributes.include = Some(include_vec);
                let source: &str = "/tmp/source";
                let target: &str = "/tmp/target";
                transfer_local2local.source = ObjectStorage::Local(source.to_string());
                transfer_local2local.target = ObjectStorage::Local(target.to_string());
                transfer_local2local.attributes.last_modify_filter = Some(LastModifyFilter {
                    filter_type: LastModifyFilterType::Greater,
                    timestamp: usize::try_from(now.as_secs()).unwrap(),
                });

                let task = Task::Transfer(transfer_local2local);

                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }
        }

        if let Some(truncate_bucket) = template.subcommand_matches("delete_bucket") {
            let file = truncate_bucket.get_one::<String>("file");
            let mut task_delete_bucket = TaskDeleteBucket::default();

            let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
            let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
            task_delete_bucket.attributes.exclude = Some(exclude_vec);
            task_delete_bucket.attributes.include = Some(include_vec);
            task_delete_bucket.attributes.last_modify_filter = Some(LastModifyFilter {
                filter_type: LastModifyFilterType::Greater,
                timestamp: usize::try_from(now.as_secs()).unwrap(),
            });

            let task = Task::DeleteBucket(task_delete_bucket);
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{:?}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{:?}", e),
                    }
                }
            };
        }

        if let Some(compare) = template.subcommand_matches("compare") {
            if let Some(oss2oss) = compare.subcommand_matches("oss2oss") {
                let file = oss2oss.get_one::<String>("file");
                let task_compare = CompareTask::default();
                let task = Task::Compare(task_compare);
                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }

            if let Some(local2oss) = compare.subcommand_matches("local2oss") {
                let file = local2oss.get_one::<String>("file");
                let mut task_compare = CompareTask::default();
                task_compare.source = ObjectStorage::Local("/tmp".to_string());
                let task = Task::Compare(task_compare);
                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }

            if let Some(oss2local) = compare.subcommand_matches("oss2local") {
                let file = oss2local.get_one::<String>("file");
                let mut task_compare = CompareTask::default();
                task_compare.target = ObjectStorage::Local("/tmp".to_string());
                let task = Task::Compare(task_compare);
                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }

            if let Some(local2local) = compare.subcommand_matches("local2local") {
                let file = local2local.get_one::<String>("file");
                let mut task_compare = CompareTask::default();
                task_compare.source = ObjectStorage::Local("/root".to_string());
                task_compare.target = ObjectStorage::Local("/tmp".to_string());
                let task = Task::Compare(task_compare);
                match file {
                    Some(f) => {
                        match struct_to_yml_file(&task, f) {
                            Ok(_) => {
                                println!("Generate {} succeed", f)
                            }
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
                        };
                    }
                    None => {
                        let yml = struct_to_yaml_string(&task);
                        match yml {
                            Ok(str) => println!("{}", str),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                };
            }
        }
    }

    if let Some(parameters) = matches.subcommand_matches("parameters") {
        if let Some(_) = parameters.subcommand_matches("provider") {
            println!("{:?}", OssProvider::JD);
            println!("{:?}", OssProvider::JRSS);
            println!("{:?}", OssProvider::ALI);
            println!("{:?}", OssProvider::S3);
            println!("{:?}", OssProvider::HUAWEI);
            println!("{:?}", OssProvider::COS);
            println!("{:?}", OssProvider::MINIO);
        }

        if let Some(_) = parameters.subcommand_matches("task_type") {
            println!("{:?}", TaskType::Transfer);
            println!("{:?}", TaskType::Compare);
        }
    }

    if let Some(gen_file) = matches.subcommand_matches("gen_file") {
        let file_size = match gen_file.get_one::<String>("file_size") {
            Some(s) => {
                let size = byte_size_str_to_usize(s);
                match size {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                }
            }
            None => {
                return;
            }
        };
        let chunk: usize = match gen_file.get_one::<String>("chunk_size") {
            Some(s) => {
                let size = byte_size_str_to_usize(s);
                match size {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                }
            }
            None => {
                return;
            }
        };

        let file = match gen_file.get_one::<String>("file_name") {
            Some(s) => s,
            None => {
                return;
            }
        };

        if let Err(e) = generate_file(file_size, chunk, file) {
            log::error!("{:?}", e);
        };
    }

    if let Some(gen_file) = matches.subcommand_matches("gen_files") {
        let dir = match gen_file.get_one::<String>("dir") {
            Some(s) => s,
            None => {
                return;
            }
        };
        let file_prefix_len: usize = match gen_file.get_one("file_prefix_len") {
            Some(s) => *s,
            None => {
                return;
            }
        };

        let file_size = match gen_file.get_one::<String>("file_size") {
            Some(s) => {
                let size = byte_size_str_to_usize(s);
                match size {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                }
            }
            None => {
                return;
            }
        };

        let chunk_size: usize = match gen_file.get_one::<String>("chunk_size") {
            Some(s) => {
                let size = byte_size_str_to_usize(s);
                match size {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                }
            }
            None => {
                return;
            }
        };

        let file_quantity: usize = match gen_file.get_one("file_quantity") {
            Some(s) => *s,
            None => {
                return;
            }
        };

        if let Err(e) = generate_files(
            dir.as_str(),
            file_prefix_len,
            file_size,
            chunk_size,
            file_quantity,
        ) {
            log::error!("{:?}", e);
        };
    }

    if let Some(_) = matches.subcommand_matches("tree") {
        let tree = get_cmd_tree(&CLIAPP);
        println!("{}", tree);
    }
}

#[cfg(test)]
mod test {
    use crate::cmd::rootcmd::{get_cmd_tree, CLIAPP};

    //cargo test cmd::rootcmd::test::test_get_command_tree -- --nocapture
    #[test]
    fn test_get_command_tree() {
        let tree = get_cmd_tree(&CLIAPP);
        println!("{}", tree);
    }
}
