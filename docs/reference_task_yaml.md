# Task yaml reference

## Transfer Task

```yaml
# 任务 id，自动生成，非必填
task_id: '7143076600257581057'
# 任务名称，非必填
name: transfer local to oss
# 任务描述
task_desc:
  # 任务类型
  type: transfer
  # 源存储，支持 本地目录或oss bucket
  source: /tmp
  # 目标存储，支持 本地目录或oss bucket
  target:
    # oss 提供方:JD,JRSS,ALI,AWS,HUAWEI,COS,MINIO
    provider: JD
    # 对象存储 access_key
    access_key_id: access_key_id
    # 对象存储 secret_access
    secret_access_key: secret_access_key
    # 对象存储endpoint，请参考供应商相关文档
    endpoint: http://s3.cn-north-1.jdcloud-oss.com
    # 对象存储区域，请参考供应商相关文档
    region: cn-north-1
    # 对象存储 bucket
    bucket: bucket_name
    # 对象存储prefix
    prefix: test/samples/
  # 任务属性  
  attributes:
    # 批次大小，既每批传输文件数据量，默认值 100
    bach_size: 100
    # 任务并行度，默认值为cpu核数
    task_parallelism: 12
    # 最大错误数，当错数量超过该值任务停止，默认值 1
    max_errors: 1
    # 任务元数据保存目录，包括任务 checkpoint、错误记录文件、文件执行列表、增量文件列表等任务执行相关文件，默认值 /tmp/meta_dir
    meta_dir: /tmp/meta_dir
    # 当目标文件存在时是否同步，用以适应某些需要跳过重复同步的场景
    target_exists_skip: false
    # 从 checkpoint 开始同步，用于任务中断后续传操作
    start_from_checkpoint: false
    # 大文件拆分依据，当文件大于该属性值时，进行查分同步，默认值50M
    large_file_size: 50m
    # 拆分块尺寸，默认值 10M
    multi_part_chunk: 10m
    # 正则表达式过滤，排除符合正则表达式的文件
    exclude:
    - test/t3/*
    - test/t4/*
    # 正则表达式过滤，包括符合正则表达式的文件
    include:
    - test/t1/*
    - test/t2/*
    # 存量模式完成后进入增量模式
    continuous: false
    transfer_type: Stock
    # 通过最后跟新时间过滤文件
    last_modify_filter:
      # 过滤方式，指定大于或小于指定时间戳，Greater、Less
      filter_type: Greater
      # unix 时间戳
      timestamp: 1703042173

```

## compare task

```yaml
# 任务 id，自动生成，非必填
task_id: '7143092364209426433'
# 任务名称，非必填
name: compare task
# 任务描述
task_desc:
  # 任务类型
  type: compare
  # 源存储，支持 本地目录或oss bucket
  source:
    # oss 提供方:JD,JRSS,ALI,AWS,HUAWEI,COS,MINIO
    provider: JD
    # 对象存储 access_key
    access_key_id: access_key_id
    # 对象存储 secret_access
    secret_access_key: secret_access_key
    # 对象存储endpoint，请参考供应商相关文档
    endpoint: http://s3.cn-north-1.jdcloud-oss.com
    # 对象存储区域，请参考供应商相关文档
    region: cn-north-1
    # 对象存储 bucket
    bucket: bucket_name
    # 对象存储prefix
    prefix: test/samples/
  # 目标存储，支持 本地目录或oss bucket
  target:
    # oss 提供方:JD,JRSS,ALI,AWS,HUAWEI,COS,MINIO
    provider: JD
    # 对象存储 access_key
    access_key_id: access_key_id
    # 对象存储 secret_access
    secret_access_key: secret_access_key
    # 对象存储endpoint，请参考供应商相关文档
    endpoint: http://s3.cn-north-1.jdcloud-oss.com
    # 对象存储区域，请参考供应商相关文档
    region: cn-north-1
    # 对象存储 bucket
    bucket: bucket_name-1
    # 对象存储prefix
    prefix: test/samples/
  # 校验检查项  
  check_option:
    # 文件长度校验
    check_content_length: true
    # 过期时间校验
    check_expires: false
    # 文件内容校验，既字节流校验
    check_content: false
    # oss meta数据校验
    check_meta_data: false
  # 任务属性  
  attributes:
    # 批次大小，既每批传输文件数据量，默认值 100
    bach_size: 100
    # 任务并行度，默认值为cpu核数
    task_parallelism: 12
    # 最大错误数，当错数量超过该值任务停止，默认值 1
    max_errors: 1
    # 任务元数据保存目录，包括任务 checkpoint、错误记录文件、文件执行列表、增量文件列表等任务执行相关文件，默认值 /tmp/meta_dir
    meta_dir: /tmp/meta_dir
    # 当目标文件存在时是否同步，用以适应某些需要跳过重复同步的场景
    target_exists_skip: false
    # 从 checkpoint 开始同步，用于任务中断后续传操作
    start_from_checkpoint: false
    # 大文件拆分依据，当文件大于该属性值时，进行查分同步，默认值50M
    large_file_size: 50m
    # 拆分块尺寸，默认值 10M
    multi_part_chunk: 10m
    # 正则表达式过滤，排除符合正则表达式的文件
    exclude:
    - ^test/t3/*
    - ^test/t4/*
    # 正则表达式过滤，包括符合正则表达式的文件
    include:
    - ^test/t1/*
    - ^test/t2/*
    # 过期时间差值，允许差值之内的误差，默认值 10 秒
    exprirs_diff_scope: 10
    # 通过最后跟新时间过滤文件
    last_modify_filter:
      # 过滤方式，指定大于或小于指定时间戳，Greater、Less
      filter_type: Greater
      # unix 时间戳
      timestamp: 1703042173
```