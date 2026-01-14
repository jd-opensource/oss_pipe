# s3proxy

## 本地存储，无校验
s3proxy.conf
```
s3proxy.ignore-unknown-headers=true
s3proxy.authorization=none
#s3proxy.cors-allow-all=true
#s3proxy.authorization=aws-v2-or-v4
#s3proxy.identity=abc
#s3proxy.credential=abcd
s3proxy.endpoint=http://0.0.0.0:8080
jclouds.provider=filesystem
jclouds.filesystem.basedir=/root/s3proxy_store
```
target/s3proxy --properties s3proxy.conf
创建 bucket
```
curl --request PUT http://localhost:8080/testbucket
```

使用 oss_pipe 上传

## 本地存储，有校验
s3proxy.conf
```
s3proxy.ignore-unknown-headers=true
#s3proxy.authorization=none
s3proxy.cors-allow-all=true
s3proxy.authorization=aws-v2-or-v4
s3proxy.identity=abc
s3proxy.credential=abcd
s3proxy.endpoint=http://0.0.0.0:8080
jclouds.provider=filesystem
jclouds.filesystem.basedir=/root/s3proxy_store
```
target/s3proxy --properties s3proxy.conf

安装 mc
```
go install github.com/minio/mc@latest
```

mc alias set s3proxy http://127.0.0.1:8080 abc abcd

mc ls s3proxy

## azure Blob
azure.conf

```
s3proxy.ignore-unknown-headers=true
s3proxy.cors-allow-all=true
s3proxy.authorization=aws-v2-or-v4
s3proxy.identity=abc
s3proxy.credential=abcd
s3proxy.endpoint=http://0.0.0.0:8080
#jclouds.azureblob.auth=azureKey
jclouds.provider=azureblob
jclouds.identity=storeaccount
jclouds.credential=xxxxxxxxxxxxxxxxxx
jclouds.endpoint=https://account.blob.core.windows.net
```

mc
az storage blob list --account-key xxxxxxxxx --account-name xxxx --container-name xxxx

上传下载通过，与 s3 交互和大文件分片待测试

## oss_pipe 配置示例

```
type: transfer
task_id: '7226145483931127809'
name: transfer_oss2local
# target: /root/files/t2
source:
  provider: JD
  access_key_id: abc
  secret_access_key: abcd
  endpoint: http://127.0.0.1:8080
  region: cn-north-1
  bucket: testbucket
target:
  provider: JD
  access_key_id: xxxx
  secret_access_key: xxxx
  endpoint: https://s3-internal.cn-north-1.jdcloud-oss.com
  region: cn-north-1
  bucket: jsw-bucket-1
attributes:
  objects_per_batch: 100
  task_parallelism: 16
  max_errors: 1
  meta_dir: /tmp/meta_dir
  target_exists_skip: false
  start_from_checkpoint: false
  large_file_size: 5m
  multi_part_chunk_size: 2m
  multi_part_chunks_per_batch: 10
  multi_part_parallelism: 32
  # exclude:
  # - test/t3/*
  # - test/t4/*
  # include:
  # - test/t1/*
  # - test/t2/*
  transfer_type: stock
```