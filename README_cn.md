# OSS PIPE

oss_pipe 是 rust 编写的文件迁移工具，旨在支撑大规模的文件迁移场景。相比 java 或 golang 构建的同类型产品，借助 rust
语言的优势，oss_pipe 具备无 GC、高并发、部署便利、OOM 风险低等优势。

## 主要功能

### transfer

文件迁移，包括 oss 间文件迁移和本地到 oss 的文件迁移

* 主要功能
    * 全量迁移
    * 存量迁移
    * 增量迁移
    * 断点续传
    * 大文件拆分上传
    * 正则表达式过滤
    * 线程数与上传快大小组合控制带宽

* 存储适配及支持列表
    * 京东云对象存储
    * 阿里云对象存储
    * 腾讯云对象存储
    * 华为云对象存储
    * AWS 对象存储
    * Minio
    * 本地

### compare

文件校验，检查源文件与迁移完成后目标文件的差异

* 主要功能
    * 存在性校验
    * 文件长度校验
    * meta 数据校验
    * 过期时间校验
    * 全字节流校验

## Getting Stated

### How to build

* 安装 rust 编译环境

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* 构建

```shell
apt update
apt install openssl
apt install libssl1.1
apt install libssl-dev
apt install -y pkg-config
```

```shell
git clone https://github.com/jiashiwen/oss_pipe.git
cd oss_pipe
cargo build --release
```

## 基本使用

oss_pipe 支持命令执行模式和交互式执行模式。
您可以通过 oss_pipe [subcommand] 执行执行任务，比如

```shell
oss_pipe parameters provider
```

也可以通过 oss_pipe -i 命里进入交互模式。交互模式支持按 'tab' 键进行子命令提示。

### 定义任务

通过 oss_pipe template 命令生成模板

```shell
oss_pipe template transfer oss2oss /tmp/transfer.yml
```

transfer.yml 文件内容

```yml
task_id: '7143066927412416513'
name: transfer oss to oss
task_desc:
  type: transfer
  source:
    provider: ALI
    access_key_id: access_key_id
    secret_access_key: secret_access_key
    endpoint: http://oss-cn-beijing.aliyuncs.com
    region: cn-north-1
    bucket: bucket_name
    prefix: test/samples/
  target:
    provider: JD
    access_key_id: access_key_id
    secret_access_key: secret_access_key
    endpoint: http://s3.cn-north-1.jdcloud-oss.com
    region: cn-north-1
    bucket: bucket_name
    prefix: test/samples/
  attributes:
    bach_size: 100
    task_parallelism: 12
    max_errors: 1
    meta_dir: /tmp/meta_dir
    target_exists_skip: false
    start_from_checkpoint: false
    large_file_size: 50m
    multi_part_chunk: 10m
    exclude:
      - test/t3/*
      - test/t4/*
    include:
      - test/t1/*
      - test/t2/*
    continuous: false
    transfer_type: Stock
    last_modify_filter:
      filter_type: Greater
      timestamp: 1703039867
```

修改 access_key_id secret_access_key 等参数，适配自己的任务。template 命令按照任务类型创建模版。parameters 支持参数查询，包括支持的
provider 以及 任务类型

### 执行任务

osstask 子命令用于执行任务

```shell
oss_pipe task exec filepath/task.yml
```

## 同步任务流程

![同步任务流程](./images/../docs/images/同步流程图-v3.png)

任务配置请参考[任务配置手册](./docs/reference_task_yml_cn.md)

命令请参考[命令手册](./docs/reference_command_cn.md)
