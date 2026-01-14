# ToDo

## 技术验证

- [x] 验证 tokio multy threads 在执行时是否阻塞

## 功能列表

### 0.2.0
- [x] 工程整理，升级依赖，剔除不必要的依赖和代码
- [x] 实现基础的 jd s3 client
- [x] 实现基础的 阿里云 s3 client
- [x] 实现 aws client
- [x] 抽象各个厂商的 oss client 形成统一的产生函数
- [x] 抽象 ossaction trait
- [x] 实现读取和上传 object bytes
- [x] jrss 适配，相关文档<http://jrss-portal-public.jdfmgt.com/>
- [x] 多线程验证 rayon 和 tokio 两个方案
- [x] 编写多线程程序
- [x] checkpoint 设计与实现 transfer
- [x] checkpoint 设计与实现 download
- [x] checkpoint 设计与实现 upload
- [x] 最大错误数机制，到达最大错误数，任务停止
- [x] 错误数据持久化与解析，配合 checkpoint 实现断点续传
- [x] 支持文件过期时间
- [x] 大文件支持<https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html>
  - [x] 大文件分片上传
  - [x] 大文件分片下载
- [x] 增加 local_to_local 任务，多线程复制目录文件
  - [x] 大文件文件流切分
  - [x] 大文件流写入
- [x] 当源端文件不存在则视作成功处理，记录 offset
- [x] 支持 oss 文件过期时间
- [x] 静态校验功能
  - [x] target 是否存在
  - [x] 过期时间差值是否在一个范围之内
  - [x] content_length 源于目标是否一致
  - [x] 精确校验模式：通过流方式读取源与目标文件，校验 buffer 是否一致，如此只消耗网络流量
- [x] 支持 last_modify_grater ,最后更改时间源端大于上次同步时获取 object list 列表的初始时间戳，配置增量参数 incremental，当该参数为真，在每次同步结束时获取本次同步起始时间戳，遍历源，生成时间戳大于该时间戳的的对象列表，执行同步任务，重复以上过程，直到 object list 内容为空
- [x] 进度展示
  - [x] 命令行进度选型
  - [x] 迁移总文件数统计
  - [x] 迁移完成文件数统计
  - [x] 计算总体进度百分比
  - [x] checkpoint 新增 total_lines 和 finished_lines
  
### 0.2.1
- [x] source analysis 功能，源端文件分析，分析大小文件所占比例，便于制定并发数量策略
- [x] upload 及 local2local 任务，通过使用 inotify 记录增量
- [x] 由于 loop 占用线程时间太长消耗系统资源，需要改进文件怎量同步部分代码，使用定时器定期轮询文件
  - [x] 定义增量记录文件，记录每次轮询的文件名及 offset
  - [x] 利用 tokio::sleep 减轻 cpu 负载
  - [x] 定时器定时按记录文件的 offset 执行增量任务
- [x] 文件上传断点续传实现
  - [x] 实现机制：先开启 notify 记录变动文件；每次断点遍历上传文件目录，取 last_modify 大于  checkpoint 记录时间戳的文件上传。需要考虑大量文件的遍历时间，需要做相关实验验证
  - [x] 兼容 minio
    - [x] minio 环境部署
    - [x] 适配 minio
  - [x] 为便于扩展，新增 objectstorage enum，用来区分不同数据源
  - [x] 解决 modify create modify 重复问题
  - [x] 增量逻辑新增正则过滤
  - [x] 将 oss 增量代码归并至 oss client
  - [x] 支持从指定时间戳同步，通过时间戳筛选大于或小于该时间戳的文件
- [x] 升级 aws 至最新版本 
- [ ] 多任务模式，统一描述文件支持多任务同事运行
- [ ] 支持 oss 存储类型，通过 set_storage_class 实现
- [x] 增加模板输出功能，增加文件名参数。
- [ ] 新增 precheck，并给出校验清单
  - [ ] 归档文件不能访问需提前校验
- [ ] 编写 makefile 实现交叉编译
- [ ] 设计多任务 snapshot，统一管理任务状态
  - [ ] 任务相关状态字段定义，任务 checkpoint，任务错误数，任务 offset map，停止标识
- [ ] 适配七牛
  - [ ] prefix map，将源 prefix 映射到目标的另一个 prefix
  - [ ] 全量、存量、增量分处理
  - [ ] 修改源为 oss 的同步机制，base taget 计算 removed 和 modified objects
- [ ] s3 multipart upload 断点续传，多快上传时，能够从未上传块开始上传 
- [ ] 验证 &mut JoinSet<()>, Arc 方式，主要验证是否可向下传递
- [ ] oss2oss multi thread upload part 改造
- [ ] 日志优化，每条记录输出任务 id；规范 info 输出格式，包括，任务 id，msg，输出类型等
- [ ] 使用 rocksdb 进行状态管理，meta_data 实现自管理
- [ ] 多任务管理
- [ ] 验证 global tokio::runtime，用于任务运行空间
- [ ] 任务限速

### split object list
- [x] 编写 object list 文件拆分相关函数，包括从 oss 获取 object，以及从文件系统获取相关的文件列表
- [x] 新增 gen_obj_list_multi_files 命令行功能，生成 meta_dir 下多个文件列表以及顺序列表和对应的第一个 checkpoint
- [] 变更 checkpoint 记录机制，根据 record 中的文件号记录执行文件，在记录时重新获取记录文件的 byts 以及行数
- [] 新增 task exec 参数 --start_from_checkpoint 便于在执行时通过参数影响任务行为
- [x] 变更代码，使用 sdk 提供的 .customize().disable_payload_signing().send() 替换.send_with_plugins(presigning)
- [ ] 将到本地的文件路径进行转换 将'//','///' 等转换为标准路径，若 key 开头为'/',拼接后进行标准化路径工作
- [ ] oss2local 路径归一化转换参数，将源路径归一化
- [ ] 增加 oss2oss 路径归一化转换参数，将源路径归一化
- [x] template 变更，增加过滤包含 http 格式的 key \b[\w-]*(https?|ftp|file):\/\/\S+
- [x] 重构流程，通过 sequenc list 遍历 object list 文件构造 record；读取 checkpoint 时按照 file_num 指定正在执行的文件
- [x] 调整 checkpoint 存储规则，以适应列表多文件
- [x] 将所有任务中的函数变更为异步函数，runtime 在 command 中指定
- [x] compare 模块改造，适应多文件列表
- [ ] compare 模型增加，按列表校验功能
- [ ] compare 自己长度时使用 head_object 减小网络通信
- [ ] 增量模式参数 IncrementMode，notify 和 scan scan 模式下 interval 参数指定轮训时间
- [x] 在任务开始之前，清理 target 上传分片
- [ ] 新增 error collect 功能用于将错误日志归集为 object list
- [x] 根据列表迁移：源端文件列表，每一行一个对象路径，遍历文件执行同步操作
- [x] 限流方案测试
      ```
      [dependencies]
      governor = "0.6"
      ```
      ```
      use governor::{Quota, RateLimiter};
      use std::num::NonZeroU32;
      
      // 创建限制器（例如 1MB/s）
      let byte_limit = NonZeroU32::new(1_000_000).unwrap();
      let quota = Quota::per_second(byte_limit);
      let limiter = RateLimiter::direct(quota);
      
      // 在发送数据前等待许可
      async fn send_data(data: &[u8]) {
          limiter.until_n_ready(data.len() as u32).await;
          // 实际发送数据...
      }
      ```
      结论，该方案只能限制强求频率无法限制流速

- [x] 验证 trickle 限流方案 方案不可用
- [ ] 增量 scan 方式把 interval 暴漏为参数
- [x] 优化 changed_object_capture_based_targe 函数，拆解步骤，新增 capture_removed_objects_to_file、capture_modified_objects_to_file，为后续并行处理做准备



q3
- [ ] 进一步优化 start_tranfer 函数，将代码进一步拆分为 execute_stock_transfer、execute_incremental_transfer 两个函数，分别执行存量和增量迁移任务
- [ ] 优化 changed_object_capture_based_targe 并发 capture_removed_objects 和 capture_modified_objects
- [ ] 新增本地新增 scan 模式，应对共享存储 notify 失效问题
- [ ] 每个模块新增 utils.rs，用于管理模块内的公共函数
- [ ] 将工程 model 集中管理，按各个模块命名，充分共享 struct
  - [ ] 归集 task、task_transfer、task_compare、task_delete
  - [ ] 归集 s3、checkpoint、filter
  - [ ] 归集 transfer_executors、compare_executors
- [x] 常量统一管理
- [ ] 多平台编译
- [] 调研-- rust 如何使用多个版本的依赖
- [x] checkpoint 增加 last_scan_timestamp 字段，记录上次扫描的时间戳，用于增量扫描时过滤已扫描的对象
  - [x] 修复 checkpoint struct
  - [ ] 修改 增量对应代码
- [ ]优化 task 执行流程，start_transfer 重构
  - [ ] 增加 init 阶段，初始化任务，检查任务状态，恢复任务进度，创建任务检查点文件
  - [ ] 增加 stock 阶段，存量数据迁移，迁移存量数据，迁移完成后，更新任务检查点文件
  - [ ] 增加 increment 阶段，增量数据迁移，迁移增量数据，迁移完成后，更新任务检查点文件
- [ ] task analyze 并发改造

  



## 文档
- [ ] 测试方案 -- 详细测试项及测试流程
- [ ] 设计文档 -- 流程及机制描述

## 校验项

- [x]last_modify 时间戳，目标大于源

## 错误处理及断点续传机制

- 日志及执行 offset 记录
  - 执行任务时每线程 offset 日志后写，记录完成的记录在文件中的 offset，offset 文件名为 文件名前缀 + 第一条记录的 offset
  - 某一记录执行出错时记录 record 及 offset，当错误数达到某一阀值，停止任务
- 断点续传机制
  - 先根据错误记录日志做补偿同步，检查源端对象是否存在，若不存在则跳过
  - 通过 offset 日志找到每个文件中的最大值，并所有文件的最大值取最小值，再与 checkpoint 中的 offset 比较取最小值，作为 objectlist 的起始 offset。

## 多个 object list 文件，限制 object list 文件的最大值，便于执行文件多的 buckt
## 增量实现

### 源为 oss 时的机制

- 回源实现方式
  - 完成一次全量同步
  - 配置对象存储回源
  - 应用切换到目标对象存储
  - 再次发起同步，只同步目标不存在或时间戳大于目标对象的对象

- 逼近法实现近似增量
  - 应用程序设置为非删除操作
  - 完成一次全量同步
  - 二次同步，只同步新增和修改 (时间戳大于目标的)，并记录更新数量
  - 知道新增更新在一个可接接受的范围，切换应用
  - 再次同步，只同步新增和修改 (时间戳大于目标的)

### 源为本地文件系统（linux）

通过 inotify 获取目录变更事件（需要验证 <https://crates.io/crates/notify）>

## 面临问题 -- sdk 兼容性问题，需要实验

- [x] 如何获取 object 属性
- [x] 如何判断 object 是否存在
- [x] 文件追加和覆盖哪个效率更高待验证

## 测试验证

- [x] tokio  实现多线程稳定性，通过循环多线程 upload 测试，异步情况需重新考虑 trait。
- [x] 验证 从 oss 流式读取行，内存直接 put 到 oss
- [ ] 验证 crossbeam_utils::sync::WaitGroup 在 async 下是否起作用

## 多线程任务

- [x] 多线程任务模型设计

## 服务化改造

- [ ] 服务化架构设计
- [ ] 任务 meta data 设计

## 分布式改造

## 日志

11 月 9
改进增量断点续传方案，抓取 target 的 remove 和  modify，执行完成后，执行增量逻辑
12 月 1
完成校验框架


问题是由于 request style 引发，变更为 path style 问题解决

The problem is caused by request style, change to path style to solve the problem

```rust
let s3_config_builder = aws_sdk_s3::config::Builder::from(&config).force_path_style(true);
let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
```

thanks!

- 2025-04-11
  - oss key 扫描存储多文件
  - 本地文件扫描存储多文件
  - checkpoint 结构变革
- 2025-04-18
  - 开发多文件列表执行传输任务的执行器
  - 变更 checkpoint 记录机制，适应多文件执行中断点续传功能
  - 重构代码，compare、transfer 构建独立模块
- 2025-04-25
  - 变更代码：移除 send_with_plugins(presigning),变更为    .customize().disable_payload_signing().send()，使用 aws sdk 标准函数
  - 使用 onecell 替换 lazy static
  - 新增 config，控制日志输出 level
- 2025-05-06
  - oss pipe 
  - compare 模块改造，适应多文件列表
  - 新增 transfer 任务开始前进行未完成的 multi part 清理
  - 将所有任务中的函数变更为异步函数，runtime 在 command 中指定，已完成 annalyze、list_objects
- 2025-05-06
  - oss pipe 
  - compare 模块改造，适应多文件列表
  - 新增 transfer 任务开始前进行未完成的 multi part 清理
  - 将所有任务中的函数变更为异步函数，runtime 在 command 中指定，已完成 annalyze、list_objects

- 2025-05-28
  - 在任务开始之前，清理 target 上传分片
  - 增加 trubleshooting 文档
  - 完成根据文件列表迁移
  
- 2025-06-4
- 验证 trickle 限流方案 方案不可用
- governor qp 限流，不符合要求


bug 修复：
compare 模块由于未阻塞线程，导致任务提前结束，部分 compare 任务未执行，bug 已修复

效能科技 asr 服务交流
  
- 新增 pre_check 模块用于在任务开始时校验源于不低端的可用性
- 变更 compare expr 模块，使用 expires_string 替换 expires(),适配新版 sdk
- 优化 changed_object_capture_based_targe 函数，拆解步骤，新增 capture_removed_objects_to_file、capture_modified_objects_to_file，为后续并行处理做准备；
- 新增 TransferTask.attributes increment_mode 字段，用于指定增量模式，目前支持 notify 和 scan
- 新增 TransferTask.attributes interval 字段，用于指定增量模式下的轮训时间


1.架构优化
为便于元数据管理、本期改造对记录传输对象的列表文件进行了拆分工作，原有单一记录文件变更为多文件记录，以保证在海量对象传输过程中单文件的传输效率问题。工程相关变更如下：
- 编写 object list 文件拆分相关函数，包括从 oss 获取 object，以及从文件系统获取相关的文件列表
- 调整 checkpoint 存储规则，以适应列表多文件
- 新增 gen_obj_list_multi_files 命令行功能，生成 meta_dir 下多个文件列表以及顺序列表和对应的第一个 checkpoint
- compare 模块改造，适应多文件列表
- 重构流程，通过 sequenc list 遍历 object list 文件构造 record；读取 checkpoint 时按照 file_num 指定正在执行的文件

2.功能优化
- 新增按列表传输功能，用户可以自行编辑传输对象列表文件
- 在任务开始之前，清理 target 上传分片
- 将所有任务中的函数变更为异步函数，runtime 在 command 中指定

3.工程优化
- 优化 changed_object_capture_based_targe 函数，拆解步骤，新增 capture_removed_objects_to_file、capture_modified_objects_to_file，为后续并行处理做准备
- 变更代码，使用 sdk 提供的 .customize().disable_payload_signing().send() 替换.send_with_plugins(presigning)








