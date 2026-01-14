# 最佳实践

## 全量迁移

- 任务编排

  通过命令  parameters provider 查看 支持的 oss 供应商
   
  
  编写任务描述文件
  ```
  type: transfer
  task_id: '7221696501834190849'
  name: transfer_oss2oss
  source:
    provider: JD
    access_key_id: access_key_id
    secret_access_key: secret_access_key
    endpoint: http://oss-cn-beijing.aliyuncs.com
    region: cn-north-1
    bucket: source_bucket_name
  target:
    provider: JD
    access_key_id: access_key_id
    secret_access_key: secret_access_key
    endpoint: http://s3.cn-north-1.jdcloud-oss.com
    region: cn-north-1
    bucket: target_bucket_name
  attributes:
    objects_per_batch: 100
    task_parallelism: 16
    max_errors: 1
    meta_dir: /tmp/meta_dir
    large_file_size: 50m
    multi_part_chunk_size: 10m
    multi_part_chunks_per_batch: 10
    multi_part_parallelism: 32 
    transfer_type: stock
  ```

- 任务源分析
  
  通过命令 oss_pipe task analyze {file.yml} 分析任务源，得到分析结果
  ```
  .--------------------------------.
  | size_range | objects | percent |
  | 0-1M       | 1       | 0.07    |
  | 300-500M   | 3       | 0.21    |
  | 1-10M      | 10      | 0.71    |
  '--------------------------------'
  ```
  分析结构会给出源端文件大小及数量的分布，可以根据这些信息调整 task_parallelism、objects_per_batch、multi_part_chunk_size、multi_part_chunks_per_batch、multi_part_parallelism 等参数。通常小文件占比越少 objects_per_batch 越大，task_parallelism 配置最好设置在 cpu 核数的 3 倍以内，multi_part_parallelism 通常与 cpu 核数相当，若迁移的大文件较多可适当增加，原则上保持在 cpu 核数的 3 倍以内。

- 执行迁移任务
  通过 oss_pipe task exec {file.yml} 命令 

## 对象过滤
在生产实践中，存在需要对源文件进行过滤的场景。oss_pipe 提供一下对象过滤的手段。

- prefix 过滤
  当远端为对象存储配置
  ```
  source:
    prefix: xxx/xxx/
  ```
  程序只对于 prefix 相符的源端对象进行操作。

- 正则表达式过滤
  ```
  attributes:
    exclude:
    - test/t3/*
    - test/t4/*
    include:
    - test/t1/*
    - test/t2/*
  ```
  任务首先通过 exclude 条件进行过滤，排除符合 exclude 条件的对象；在 exclude 过滤的结果集中筛选符合 include 条件的对象。

- 时间戳过滤
  ```
  attributes:
    last_modify_filter:
        filter_type: Greater
        timestamp: 1721789609

  ```
  oss_pipe 可以通过文件最后修改的时间戳作为条件进行过滤。可以通过定义 filter_type，选择 Greater(大于等于) 或 Less(小于等于) 时间戳来定义过滤条件

## 利用回源功能迁移
oss_pipe 本身支持增量数据迁移，但在 bucket 数据量过于庞大时，轮训时间较长。这时可以通过对象存储本身的回源功能实现迁移动作。
步骤如下：
- 进行存量迁移
- 配置目标端回源 (详细步骤参考对象存储提供商文档)
- 切换应用到目标端
- 通过时间戳过滤迁移远端增量部分，时间戳设为首次迁移的起始时间 v

## 异常中断处理
在文件迁移过程中，经常遇到由于网络或其他原因任务中断的情况。可以通过配置一下参数后重新启动任务进行任务续接。
```
attributes:
  start_from_checkpoint: ture
```
当 start_from_checkpoint 为 true。当任务处于存量阶段时启动任务后任务会从最近的检查点开始同步任务；当任务处于增量阶段，任务将根据 checkpoint 中的时间戳同步时间戳晚于最后一次操作的时间戳的对象，执行完成后进入增量逻辑
