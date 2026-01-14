# 错误处理

在实际迁移操作中会碰到由于环境或数据异常造成的任务意外中断情况，大致分为一下两种情况
- 网络异常
- 数据异常

## 网络异常
- 网络中断引起的异常
  将任务描述文件中 start_from_checkpoint 设置为 true，重新启动任务即可
  ```
  attributes:
    start_from_checkpoint: true 
  ```
- 由于并发过高引起的服务端超时
  可降低任务中的并发参数并将 start_from_checkpoint 设置为 true，重新启动，相关参数示例如下
  ```
  attributes:
    start_from_checkpoint: true
    objects_per_batch: 64
    task_parallelism: 2
    start_from_checkpoint: true
    large_file_size: 128m
    multi_part_chunk: 32m
    multi_part_max_parallelism: 4
  ```

## 数据异常
数据异常通常是由于不同对象存储对于 key 的校验规则不一致导致。例如某些对象存储支持类似"a//b///c"这种 key 类型而 target 端将其视为错误路径。
处理办法：
- 保证任务正常进行
  进入 meta_dir 目录，参考配置文件中的设置
  ```
  attributes:
    meta_dir: /tmp/meta_dir/xxxx
  ```
  目录中 list_files 目录中记录所有要迁移的 key。通常，在报错信息中会给出 key 的名字以及所在文件的行号，对照该信息备份该记录后从列表文件中删除该条记录，将  start_from_checkpoint: true，重新启动任务，保证任务正常进行。

- 数据补传
  可将格式有问题的对象通过 [mc](https://github.com/minio/mc) 命令将对象下载到本地，在使用正确的路径上传至目标


