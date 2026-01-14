# Getting Stated

## How to build

* 安装 rust 编译环境

```rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```


* 依赖安装

- ubuntu 22 x86_64
```shell
apt update \
&& apt install openssl -y \
&& apt install libssl-dev \
&& apt install -y pkg-config
```

- centos 8 x86_64
   需重新编译
```shell
yum install git gcc -y
```

- Rocky 9 x86_64
```shell
yum install git gcc -y
```

- Kylin V10 SP3
  需重新编译
```shell
yum install git gcc -y
```

- OpenEuler 22.03 x86_64
```shell
yum install git gcc -y
```

- OpenCloudOS 8.6 x86_64
 需重新编译
```shell
yum install git gcc -y
```

- UOS v20 x86_64
```shell
yum install git gcc -y
```

- ubuntu 20 arm
```shell
apt update \
&& apt install openssl -y \
&& apt install libssl-dev \
&& apt install -y pkg-config
```

- OpenEuler 22.03 arm
```shell
yum install git gcc -y
```

* 构建
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
task_id: '7132608357105537025'
name: transfer oss to oss
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
  objects_per_batch: 100
  task_parallelism: 11
  max_errors: 1
  meta_dir: /tmp/meta_dir
  target_exists_skip: false
  start_from_checkpoint: false
  large_file_size: 50m
  multi_part_chunk_size: 10m
  multi_part_chunks_per_batch: 10
  multi_part_parallelism: 22
```

修改 access_key_id secret_access_key 等参数，适配自己的任务。template 命令按照任务类型创建模版，模板描述请参考[参考手册](reference_task_yml_cn.md)。parameters 支持参数查询，包括支持的 provider 以及 任务类型

### 执行任务

osstask 子命令用于执行任务

```shell
oss_pipe task exec filepath/task.yml
```

[命令行参考](reference_command_cn.md)
## 同步任务流程

![同步任务流程](./images/同步流程图-v3.png)
