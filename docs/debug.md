# Debug

## multi part upload cpu 高

```
 let mut part_number = 0;
    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let file_len = file.metadata().unwrap().len();
    let mut readed = 0;


    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().unwrap();
    println!("upload_id is {}", upload_id);

    loop {
        let mut buf = vec![0; chunk_size];
        let read_count = file.read(&mut buf).unwrap();
        readed += read_count;
        println!("readed/total:{}/{}", readed, file_len);
        part_number += 1;

        if read_count == 0 {
            break;
        }

        let body = &buf[..read_count];
        let stream = ByteStream::from(body.to_vec());
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;

        let completed_part = CompletedPart::builder()
            .e_tag(upload_part_res.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build();

        upload_parts.push(completed_part);

        if read_count != chunk_size {
            break;
        }
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
```

aws_sigv4::http_request::canonical_request::CanonicalRequest::payload_hash 占用 70%
aws rust sdk runtime_plugins 字段为私有，不可变更，提 issue


flamegraph -o flamegraph_00.svg --pid  10073

修改建议
为 UploadPartInputBuilder、PutObject、GetObjectInputBuilder 新增 set_payload_override，以便使用 aws_sigv4::http_request::SignableBody 改变对象传递时频繁调用 sigv4 造成 cpu 使用率过高的问题

```
      let payload_override = aws_sigv4::http_request::SignableBody::UnsignedPayload;
      let upload_part_res = client
            .upload_part()
            .set_payload_override(payload_override)
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(stream)
            .part_number(p.part_num)
            .send()
            .await?;
```

```
impl UploadPartInputBuilder {
     pub fn set_payload_override(mut self, payload_override: ::aws_sigv4::http_request::SignableBody) -> Self {
        self.inner.signing_options.payload_override = payload_override;;
        self
    }
}
```

payload_override: aws_sigv4::http_request::SignableBody::UnsignedPayload,


Added set_payload_override to UploadPartInputBuilder, PutObject, and GetObjectInputBuilder to prevent frequent calls to sigv4 when using aws_sigv4::http_request::SignableBody to change object transfers, causing high CPU usage.


modify sdk/s3/src/operation/upload_part/builders.rs

```
use ::aws_smithy_runtime_api::client::result;
use crate::presigning::PresigningConfig;

impl UploadPartFluentBuilder {
    pub async fn send(
        self,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        ::aws_smithy_runtime_api::client::result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(::aws_smithy_runtime_api::client::result::SdkError::construction_failure)?;
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle.runtime_plugins.clone().with_client_plugin(
                crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
                    crate::presigning::PresigningConfig::expires_in(
                        std::time::Duration::from_secs(3000),
                    )
                    .unwrap(),
                    ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
                ),
            ),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }


    /// Sends the request and returns the response with clinet plugin.
    ///
    /// If an error occurs, an `SdkError` will be returned with additional details that
    /// can be matched against.
    pub async fn send_with_plugins(
        self,
        presigning_config: PresigningConfig,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        ::aws_smithy_runtime_api::client::result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(::aws_smithy_runtime_api::client::result::SdkError::construction_failure)?;

        let plugin = crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
            presigning_config,
            ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
        );
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle
                .runtime_plugins
                .clone()
                .with_client_plugin(plugin),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }



}

```



我已经通过修改 sdk/s3/src/operation/upload_part/builders.rs 假如新函数 send_with_plugins 实现了这个功能。
修改代码如下：

```
use ::aws_smithy_runtime_api::client::result;
use crate::presigning::PresigningConfig;

impl UploadPartFluentBuilder {
    pub async fn send(
        self,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        ::aws_smithy_runtime_api::client::result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(::aws_smithy_runtime_api::client::result::SdkError::construction_failure)?;
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle.runtime_plugins.clone().with_client_plugin(
                crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
                    crate::presigning::PresigningConfig::expires_in(
                        std::time::Duration::from_secs(3000),
                    )
                    .unwrap(),
                    ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
                ),
            ),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }


    /// Sends the request and returns the response with clinet plugin.
    ///
    /// If an error occurs, an `SdkError` will be returned with additional details that
    /// can be matched against.
    pub async fn send_with_plugins(
        self,
        presigning_config: PresigningConfig,
    ) -> ::std::result::Result<
        crate::operation::upload_part::UploadPartOutput,
        result::SdkError<
            crate::operation::upload_part::UploadPartError,
            ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >,
    > {
        let input = self
            .inner
            .build()
            .map_err(result::SdkError::construction_failure)?;

        let plugin = crate::presigning_interceptors::SigV4PresigningRuntimePlugin::new(
            presigning_config,
            ::aws_sigv4::http_request::SignableBody::UnsignedPayload,
        );
        let runtime_plugins = crate::operation::upload_part::UploadPart::operation_runtime_plugins(
            // self.handle.runtime_plugins.clone(),
            self.handle
                .runtime_plugins
                .clone()
                .with_client_plugin(plugin),
            &self.handle.conf,
            self.config_override,
        );
        crate::operation::upload_part::UploadPart::orchestrate(&runtime_plugins, input).await
    }

}
```

使用用例

```
let presigning = PresigningConfig::expires_in(std::time::Duration::from_secs(3000))?;
        let upload_part_res = self
            .client
            .upload_part()
            .upload_id(upload_id)
            .part_number(part_number)
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send_with_plugins(presigning)
            .await?;
```

flamegraph -o flamegraph_01.svg --pid 2017221
Duration { seconds: 2524, nanoseconds: 680871254 }


## 使用 /etc/hosts 绑定，部分 api 不可用

当使用 /etc/hosts 进行绑定时，部分 api 报错，这里分别验证了 list_buckets，list_objects_v2,get_object

```rust
#[tokio::main]
async fn main() {
    let ak = "xxx";
    let sk = "xxx";
    let region = "cn-north-1";
    let endpoint = "http://xxx.cn-north-1.xxx.com";
    let bucket = "jsw-bucket";
    // let endpoint = "http://100.64.130.88";

    // let ep = Endpoint::builder().url(endpoint).build();
    // let ep = Endpoint::immutable(Uri::from_static(endpoint));
    let config = SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            ak, sk, None, None, "Static",
        )))
        .endpoint_url(endpoint)
        .region(Region::new(region))
        .behavior_version(BehaviorVersion::v2024_03_28())
        .build();

    let s3_config_builder = aws_sdk_s3::config::Builder::from(&config);
    let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

    let buckets = client.list_buckets().send().await;
    println!("got buckets: {:#?}", buckets);

    let list = client
        .list_objects_v2()
        .bucket("jsw-bucket")
        .max_keys(20)
        .send()
        .await;
    println!("objects: {:#?}", list);

    let obj = client
        .get_object()
        .bucket(bucket)
        .key("100k/!!41725505223551787799")
        .send()
        .await;
    println!("obj: {:#?}", obj);
}
```

此时关闭 DNS，/etc/hosts 配置
```
ip xxx.cn-north-1.xxx.com
```

报错如下
```
got buckets: Ok(
    ListBucketsOutput {
        buckets: Some(
            [
                Bucket {
                    name: Some(
                        "ai-datasets",
                    ),
                    creation_date: Some(
                        2024-02-05T02:28:04Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "cs-monitor",
                    ),
                    creation_date: Some(
                        2023-02-07T10:12:00Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "dataroom",
                    ),
                    creation_date: Some(
                        2024-05-16T10:26:42Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "hackthon2023",
                    ),
                    creation_date: Some(
                        2023-08-01T01:43:57Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "jsw-bucket",
                    ),
                    creation_date: Some(
                        2023-02-23T03:11:46Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "jsw-bucket-1",
                    ),
                    creation_date: Some(
                        2023-09-20T09:43:15Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "pingdata",
                    ),
                    creation_date: Some(
                        2021-08-27T03:35:08Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "robot-test",
                    ),
                    creation_date: Some(
                        2022-04-12T06:41:43Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "robots",
                    ),
                    creation_date: Some(
                        2021-11-09T03:03:23Z,
                    ),
                },
                Bucket {
                    name: Some(
                        "tsp-picture",
                    ),
                    creation_date: Some(
                        2022-01-10T03:17:10Z,
                    ),
                },
            ],
        ),
        owner: Some(
            Owner {
                display_name: Some(
                    "jcloud_edUmprJ",
                ),
                id: Some(
                    "577257366345",
                ),
            },
        ),
        continuation_token: None,
        _extended_request_id: None,
        _request_id: Some(
            "917104E844BBC1FA",
        ),
    },
)
objects: Err(
    DispatchFailure(
        DispatchFailure {
            source: ConnectorError {
                kind: Io,
                source: hyper::Error(
                    Connect,
                    ConnectError(
                        "dns error",
                        Custom {
                            kind: Uncategorized,
                            error: "failed to lookup address information: Name or service not known",
                        },
                    ),
                ),
                connection: Unknown,
            },
        },
    ),
)
obj: Err(
    DispatchFailure(
        DispatchFailure {
            source: ConnectorError {
                kind: Io,
                source: hyper::Error(
                    Connect,
                    ConnectError(
                        "dns error",
                        Custom {
                            kind: Uncategorized,
                            error: "failed to lookup address information: Name or service not known",
                        },
                    ),
                ),
                connection: Unknown,
            },
        },
    ),
)
```

某些 api 只通过解析 DNS 而不能通过 /etc/hosts 绑定域名吗

解决办法：通过 set force_path_style(true) 解决问题

## s3 功能升级引发的错误

- 报错信息
```
Caused by:
    0: service error
    1: unhandled error (NotImplemented)
    2: Error { code: "NotImplemented", message: "A header you provided implies x-amz-trailer operation which is not implemented" } RecordOption { source_key: "512k/!%X1727344854604616419", target_key: "512k/!%X1727344854604616419", list_file_path: "/tmp/meta_dir/transfer_objects_list_1743387599", list_file_position: FilePosition { offset: 5331, line_num: 192 }, option: PUT }
```
- 原因
  aws sdk 升级后部分 server 端 api 没有及时升级

- 临时解决方案
  将 aws sdk rust 版本进行回退

  ```
  cd aws-sdk-rust
  git log --oneline --date=iso
  ```

  2025-01-14 以后的版本没有这个问题 
  b51c82e1f3c (tag: release-2025-01-14) Update changelog and tag release manifest
  ```
  git reset --hard b51c82e1f3c
  ```


