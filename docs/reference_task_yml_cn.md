# 任务描述参考手册

oss_pipe 通过 yml 格式描述需要执行的任务

## yaml 描述文件基本结构

### 描述文件概述
任务描述主要分为三个主要部分
- type、task_id、name，用于定义任务的基本信息，type 为类型描述，目前支持 transfer 和 compare 两类任务。
- source、targe 用于描述源于目标存储信息，可以是本地存储（目录）或对象存储
- attributes，用于描述任务属性，包括并发数、过滤器等信息

基本属性描述
- type: 描述任务类型，目前支持 transfer 和 compare 两类任务
- task_id：描述任务唯一 id 以及任务名称，
- name：任务名称，使用者自定义
- source: 描述远端存储类别
- target: 描述目标端存储类别
- attributes: 描述任务属性



### transfer yaml
```
type: transfer
task_id: '7322221674433220609'
name: transfer_oss2oss
source:
  provider: ALI
  access_key_id: access_key_id
  secret_access_key: secret_access_key
  endpoint: http://oss-cn-beijing.aliyuncs.com
  region: cn-north-1
  bucket: bucket_name
  prefix: test/samples/
  request_style: VirtualHostedStyle
target:
  provider: JD
  access_key_id: access_key_id
  secret_access_key: secret_access_key
  endpoint: http://s3.cn-north-1.jdcloud-oss.com
  region: cn-north-1
  bucket: bucket_name
  prefix: test/samples/
  request_style: VirtualHostedStyle
attributes:
  objects_per_batch: 64
  task_parallelism: 4
  meta_dir: /tmp/meta_dir
  target_exists_skip: false
  start_from_checkpoint: false
  large_file_size: 64m
  multi_part_chunk_size: 8m
  multi_part_chunks_per_batch: 16
  multi_part_parallelism: 8
  multi_part_max_parallelism: 12
  exclude:
  - \b[\w-]*(https?|ftp|file):\/\/\S+
  - test/t4/*
  include:
  - test/t1/*
  - test/t2/*
  transfer_type: stock
  last_modify_filter:
    filter_type: Greater
    timestamp: 1745753687
  objects_list_files_max_line: 1000000
```


#### transfer yml 参数详解

<table>
    <tr>
	  <td >配置项</td>
	  <td>字段属性</td>
	  <td>必填</td>  
      <td>描述</td>
      <td>示例</td>   
	</tr >
    <tr>
	   <td> type </td>
	   <td>String</td>
       <td>是</td>
       <td>任务类型，transfer 或 compare，详细执行类型通过：oss_pipe  parameters task_type 查看</td>
       <td>type: transfer/compare</td>
	</tr>
     <tr>
	   <td>task_id</td>
	   <td>String</td>
       <td>否</td>
       <td>任务 id，为空时由系统生成</td>
       <td>task_id: '7219552894540976129'</td>
	</tr>
    <tr>
	   <td>name</td>
	   <td>String</td>
       <td>否</td>
       <td>任务名称，为空时系统生成默认名称</td>
       <td>name: transfer_oss2oss</td>
	</tr>
    <tr>
	   <td>source</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为本地目录时，指定本地目录</td>
       <td>source: /tmp/source_files</td>
	</tr>
    <tr>
	   <td>source.provider</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，描述对象存储提供商。值为 JD/JRSS/ALI/AWS/HUAWEI/COS/MINIO，支持的对象存储提供商通过 oss_pipe parameters provider 查询</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;provider: AWS</td>
	</tr>
    <tr>
	   <td>source.access_key_id</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 access_key。</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;access_key_id: xxxx</td>
	</tr>
    <tr>
	   <td>source.secret_access_key</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 secret key</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp; secret_access_key: xxxx</td>
	</tr>
    <tr>
	   <td>source.endpoint</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储 endpoint</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;endpoint: http://oss-cn-beijing.aliyuncs.com</td>
	</tr>
    <tr>
	   <td>source.region</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储 region</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;region: cn-north-1</td>
	</tr>
    <tr>
	   <td>source.bucket</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储 bucket</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;bucket: bucket_name</td>
	</tr>
    <tr>
	   <td>source.prefix</td>
	   <td>String</td>
       <td>否</td>
       <td>当源为对象存储时，指定 prefix 时，只对该 prefix 下的对象进行操作</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;prefix: source/test/prefix/</td>
	</tr>
      <tr>
	   <td>source.request_style</td>
	   <td>String</td>
       <td>否</td>
       <td>对象存储 url 编码格式，取值：PathStyle/VirtualHostedStyle，默认 VirtualHostedStyle</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;request_style: VirtualHostedStyle</td>
	</tr>
    <tr>
	   <td>target</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为本地目录时，指定本地目录</td>
       <td>target: /tmp/target_files</td>
	</tr>
    <tr>
	   <td>target.provider</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，描述对象存储提供商。值为 JD/JRSS/ALI/AWS/HUAWEI/COS/MINIO，支持的对象存储提供商通过 oss_pipe parameters provider 查询</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;provider: AWS</td>
	</tr>
    <tr>
	   <td>target.access_key_id</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，指定对象存储的 access_key。</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;access_key_id: xxxx</td>
	</tr>
    <tr>
	   <td>target.secret_access_key</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 secret key</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp; secret_access_key: xxxx</td>
	</tr>
    <tr>
	   <td>target.endpoint</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储 endpoint</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;endpoint: http://oss-cn-beijing.aliyuncs.com</td>
	</tr>
    <tr>
	   <td>target.region</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储 region</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;region: cn-north-1</td>
	</tr>
    <tr>
	   <td>target.bucket</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储 bucket</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;bucket: bucket_name</td>
	</tr>
    <tr>
	   <td>target.prefix</td>
	   <td>String</td>
       <td>否</td>
       <td>当目标为对象存储时，指定 prefix 时，目标添加 prefix，例如源 key 为 a，指定 prefix 为 p/时，a 在目标的 key 为 p/a</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;prefix: target/prefix/</td>
	</tr>
    <tr>
	   <td>target.request_style</td>
	   <td>String</td>
       <td>否</td>
       <td>对象存储 url 编码格式，取值：PathStyle/VirtualHostedStyle，默认 VirtualHostedStyle</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;request_style: VirtualHostedStyle</td>
	</tr>
    <tr>
	   <td>attributes.objects_transfer_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_transfer_batch: 100</td>
	</tr>
    <tr>
	   <td>attributes.task_parallelism</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，任务并行度，既同时执行任务批次的数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;task_parallelism: 16</td>
	</tr>
    <tr>
	   <td>attributes.objects_list_batch</td>
	   <td>usize</td>
       <td>否</td>
       <td>任务属性，对象列表批次，每批次写入列表文件的数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_batch: 1000</td>
	</tr>
    <tr>
	   <td>attributes.meta_dir</td>
	   <td>String</td>
       <td>否</td>
       <td>任务属性，元数据存储位置，默认路径/tmp/meta_dir</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;meta_dir: /root/meta_dir</td>
	</tr>
    <tr>
	   <td>attributes.target_exists_skip</td>
	   <td>bool</td>
       <td>否</td>
       <td>任务属性，当 target 存在同名对象时不传送对象，默认值为 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;target_exists_skip: false</td>
	</tr>
    <tr>
	   <td>attributes.start_from_checkpoint</td>
	   <td>bool</td>
       <td>否</td>
       <td>任务属性，是否从 checkpoint 开始执行任务，用于任务中断后接续执行，默认值 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;start_from_checkpoint: true</td>
	</tr>
    <tr>
	   <td>attributes.large_file_size</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，超过该参数设置尺寸的文件，文件切片传输</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;large_file_size: 50M</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_chunk_size</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，对象分片尺寸</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;multi_part_chunk_size: 10m</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_chunks_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批执行的分片数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp; multi_part_chunks_per_batch: 20</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_parallelism</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，分片批次执行的并行度</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;multi_part_parallelism: 8</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_max_parallelism</td>
	   <td>usize</td>
       <td>否</td>
       <td>任务属性，分片上传的最大并行度</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;multi_part_max_parallelism: 12</td>
	</tr>
    <tr>
	   <td>attributes.exclude</td>
	   <td>list</td>
       <td>否</td>
       <td>任务属性，配置排除对象的正则表达式列表，符合列表的对象将不被处理</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;oexclude: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t3/* <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t4/*</td>
	</tr>
    <tr>
	   <td>attributes.include</td>
	   <td>list</td>
       <td>是</td>
       <td>任务属性，配置正则表达式列表，程序只处理符合列表的对象</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;include: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t3/* <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t4/*</td></td>
	</tr>
    <tr>
	   <td>attributes.transfer_type</td>
	   <td>String</td>
       <td>是</td>
       <td>任务属性，传输类型 stock/full,目前支持存量和全量模式</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;transfer_type: stock/full</td>
	</tr>
    <tr>
	   <td>attributes.last_modify_filter</td>
	   <td>usize</td>
       <td>否</td>
       <td>任务属性，根据需要筛选符合实际戳条件的对象进行传输</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;last_modify_filter: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;filter_type: Greater/Less <br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;timestamp: 1721284711</td>
	</tr>
        <tr>
	   <td>attributes.objects_list_batch</td>
	   <td>i32</td>
       <td>否</td>
       <td>获取传输列表时，每批次获取对象的数量，默认值 512，当源为对象存储时最大 1000</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_batch: 512</td>
	</tr>
    <tr>
	   <td>attributes.objects_list_file_max_line</td>
	   <td>i32</td>
       <td>否</td>
       <td>列表文件容纳的最大行数，超过自动填充下一个列表文件</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_file_max_line: 100000</td>
	</tr>
    <tr>
	   <td>attributes.objects_list_files</td>
	   <td>list</td>
       <td>否</td>
       <td>任务属性，手动指定列表文件</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_files: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- /path/to/list1.txt <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- /path/to/list2.txt</td>
	</tr>
    <tr>
	   <td>attributes.increment_mode</td>
	   <td>String</td>
       <td>否</td>
       <td>任务属性，增量模式：scan 或 notify</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;increment_mode: scan</td>
	</tr>    
</table>

### compare yaml
```
type: compare
task_id: '7350354445546426369'
name: default_name
source:
  provider: JD
  access_key_id: access_key_id
  secret_access_key: secret_access_key
  endpoint: http://s3.cn-north-1.jdcloud-oss.com
  region: cn-north-1
  bucket: bucket_name
  prefix: test/samples/
  request_style: VirtualHostedStyle
target:
  provider: JD
  access_key_id: access_key_id
  secret_access_key: secret_access_key
  endpoint: http://s3.cn-north-1.jdcloud-oss.com
  region: cn-north-1
  bucket: bucket_name
  prefix: test/samples/
  request_style: VirtualHostedStyle
check_option:
  check_content_length: true
  check_expires: false
  check_content: false
  check_meta_data: false
attributes:
  objects_per_batch: 64
  task_parallelism: 16
  meta_dir: /tmp/meta_dir
  start_from_checkpoint: false
  large_file_size: 64m
  multi_part_chunk: 8m
  multi_part_max_parallelism: 12
  exclude: null
  include: null
  exprirs_diff_scope: 10
  last_modify_filter: null
  objects_list_batch: 512
  objects_list_files_max_line: 1000000
```

#### compare yml 参数详解

<table>
    <tr>
	  <td >配置项</td>
	  <td>字段属性</td>
	  <td>必填</td>  
      <td>描述</td>
      <td>示例</td>   
	</tr >
    <tr>
	   <td> type </td>
	   <td>String</td>
       <td>是</td>
       <td>任务类型，transfer 或 compare，详细执行类型通过：oss_pipe  parameters task_type 查看</td>
       <td>type: transfer/compare</td>
	</tr>
     <tr>
	   <td>task_id</td>
	   <td>String</td>
       <td>否</td>
       <td>任务 id，为空时由系统生成</td>
       <td>task_id: '7219552894540976129'</td>
	</tr>
    <tr>
	   <td>name</td>
	   <td>String</td>
       <td>否</td>
       <td>任务名称，为空时系统生成默认名称</td>
       <td>name: transfer_oss2oss</td>
	</tr>
    <tr>
	   <td>source</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为本地目录时，指定本地目录</td>
       <td>source: /tmp/source_files</td>
	</tr>
    <tr>
	   <td>source.provider</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，描述对象存储提供商。值为 JD/JRSS/ALI/AWS/HUAWEI/COS/MINIO，支持的对象存储提供商通过 oss_pipe parameters provider 查询</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;provider: AWS</td>
	</tr>
    <tr>
	   <td>source.access_key_id</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 access_key。</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;access_key_id: xxxx</td>
	</tr>
    <tr>
	   <td>source.secret_access_key</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 secret key</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp; secret_access_key: xxxx</td>
	</tr>
    <tr>
	   <td>source.endpoint</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储 endpoint</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;endpoint: http://oss-cn-beijing.aliyuncs.com</td>
	</tr>
    <tr>
	   <td>source.region</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储 region</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;region: cn-north-1</td>
	</tr>
    <tr>
	   <td>source.bucket</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，对象存储 bucket</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;bucket: bucket_name</td>
	</tr>
    <tr>
	   <td>source.prefix</td>
	   <td>String</td>
       <td>否</td>
       <td>当源为对象存储时，指定 prefix 时，只对该 prefix 下的对象进行操作</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;prefix: source/test/prefix/</td>
	</tr>
      <tr>
	   <td>source.request_style</td>
	   <td>String</td>
       <td>否</td>
       <td>对象存储 url 编码格式，取值：PathStyle/VirtualHostedStyle，默认 VirtualHostedStyle</td>
       <td>source:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;request_style: VirtualHostedStyle</td>
	</tr>
    <tr>
	   <td>target</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为本地目录时，指定本地目录</td>
       <td>target: /tmp/target_files</td>
	</tr>
    <tr>
	   <td>target.provider</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，描述对象存储提供商。值为 JD/JRSS/ALI/AWS/HUAWEI/COS/MINIO，支持的对象存储提供商通过 oss_pipe parameters provider 查询</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;provider: AWS</td>
	</tr>
    <tr>
	   <td>target.access_key_id</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，指定对象存储的 access_key。</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;access_key_id: xxxx</td>
	</tr>
    <tr>
	   <td>target.secret_access_key</td>
	   <td>String</td>
       <td>是</td>
       <td>当源为对象存储时，指定对象存储的 secret key</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp; secret_access_key: xxxx</td>
	</tr>
    <tr>
	   <td>target.endpoint</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储 endpoint</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;endpoint: http://oss-cn-beijing.aliyuncs.com</td>
	</tr>
    <tr>
	   <td>target.region</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储 region</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;region: cn-north-1</td>
	</tr>
    <tr>
	   <td>target.bucket</td>
	   <td>String</td>
       <td>是</td>
       <td>当目标为对象存储时，对象存储 bucket</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;bucket: bucket_name</td>
	</tr>
    <tr>
	   <td>target.prefix</td>
	   <td>String</td>
       <td>否</td>
       <td>当目标为对象存储时，指定 prefix 时，目标添加 prefix，例如源 key 为 a，指定 prefix 为 p/时，a 在目标的 key 为 p/a</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;prefix: target/prefix/</td>
	</tr>
    <tr>
	   <td>target.request_style</td>
	   <td>String</td>
       <td>否</td>
       <td>对象存储 url 编码格式，取值：PathStyle/VirtualHostedStyle，默认 VirtualHostedStyle</td>
       <td>target:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;request_style: VirtualHostedStyle</td>
	</tr>
    <tr>
	   <td>check_option.check_content_length</td>
	   <td>usize</td>
       <td>否</td>
       <td>校验属性，是否校验内容长度，默认为 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;check_content_length: false</td>
	</tr>
    <tr>
    	<td>check_option.check_expire</td>
	   <td>usize</td>
       <td>否</td>
       <td>校验属性，是否校验过期时间，当源和目标均为对象存储时起作用，默认为 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;check_expire: false</td>
	</tr>
    <tr>
    	<td>check_option.check_meta_data</td>
	   <td>usize</td>
       <td>否</td>
       <td>是否校验 meta data，当源和目标均为对象存储时生效，默认为 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;check_meta_data: false</td>
	</tr>   
    <tr>
    	<td>check_option.check_content</td>
	   <td>usize</td>
       <td>否</td>
       <td>是否校验文件内容，开启该配置会对文件内容按字节进行校验，流量消耗大，慎重开启，默认为 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;check_content: false</td>
	</tr>     
    <tr>
	   <td>attributes.objects_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批次执行的对象数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_per_batch: 100</td>
	</tr>
    <tr>
	   <td>attributes.task_parallelism</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，任务并行度，既同时执行任务批次的数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;task_parallelism: 16</td>
	</tr>
    <tr>
	   <td>attributes.objects_list_batch</td>
	   <td>usize</td>
       <td>否</td>
       <td>任务属性，对象列表批次，每批次写入列表文件的数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_batch: 1000</td>
	</tr>
    <tr>
	   <td>attributes.meta_dir</td>
	   <td>String</td>
       <td>否</td>
       <td>任务属性，元数据存储位置，默认路径/tmp/meta_dir</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;meta_dir: /root/meta_dir</td>
	</tr>
    <tr>
	   <td>attributes.target_exists_skip</td>
	   <td>bool</td>
       <td>否</td>
       <td>任务属性，当 target 存在同名对象时不传送对象，默认值为 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;target_exists_skip: false</td>
	</tr>
    <tr>
	   <td>attributes.start_from_checkpoint</td>
	   <td>bool</td>
       <td>否</td>
       <td>任务属性，是否从 checkpoint 开始执行任务，用于任务中断后接续执行，默认值 false</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;start_from_checkpoint: true</td>
	</tr>
    <tr>
	   <td>attributes.large_file_size</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，超过该参数设置尺寸的文件，文件切片传输</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;large_file_size: 50M</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_chunk_size</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，对象分片尺寸</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;multi_part_chunk_size: 10m</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_chunks_per_batch</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，每批执行的分片数量</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp; multi_part_chunks_per_batch: 20</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_parallelism</td>
	   <td>usize</td>
       <td>是</td>
       <td>任务属性，分片批次执行的并行度</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;multi_part_parallelism: 8</td>
	</tr>
    <tr>
	   <td>attributes.multi_part_max_parallelism</td>
	   <td>usize</td>
       <td>否</td>
       <td>任务属性，分片上传的最大并行度</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;multi_part_max_parallelism: 12</td>
	</tr>
    <tr>
	   <td>attributes.exclude</td>
	   <td>list</td>
       <td>否</td>
       <td>任务属性，配置排除对象的正则表达式列表，符合列表的对象将不被处理</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;oexclude: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t3/* <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t4/*</td>
	</tr>
    <tr>
	   <td>attributes.include</td>
	   <td>list</td>
       <td>是</td>
       <td>任务属性，配置正则表达式列表，程序只处理符合列表的对象</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;include: <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t3/* <br>
        &nbsp;&nbsp;&nbsp;&nbsp;- test/t4/*</td></td>
	</tr>
    	</tr>
        <tr>
	   <td>attributes.exprirs_diff_scope</td>
	   <td>i32</td>
       <td>否</td>
       <td>当校验过期时间时由于服务器间的时间差异需要一定冗余，既相差在一定时间内既为校验成功，默认相差 10 秒以内</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;exprirs_diff_scope: 10</td>
	</tr>
	</tr>
    	</tr>
        <tr>
	   <td>attributes.objects_list_batch</td>
	   <td>i32</td>
       <td>否</td>
       <td>获取传输列表时，每批次获取对象的数量，默认值 512，当源为对象存储时最大 1000</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_batch: 512</td>
	</tr>
    <tr>
	   <td>attributes.objects_list_file_max_line</td>
	   <td>i32</td>
       <td>否</td>
       <td>列表文件容纳的最大行数，超过自动填充下一个列表文件</td>
       <td>attributes:<br>
        &nbsp;&nbsp;&nbsp;&nbsp;objects_list_file_max_line: 100000</td>
	</tr>
       
</table>