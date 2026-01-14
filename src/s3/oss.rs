use crate::models::model_s3::{OSSDescription, OssProvider, S3RequestStyle};

use super::{jd_s3::OssJdClient, oss_client::OssClient};
use anyhow::Result;
use async_trait::async_trait;
use aws_config::{
    retry::RetryConfig,
    timeout::{TimeoutConfig, TimeoutConfigBuilder},
    BehaviorVersion, SdkConfig,
};
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::config::{Region, StalledStreamProtectionConfig};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[async_trait]
pub trait OSSActions {
    fn oss_client_type(&self) -> OssProvider;

    // 按批次获取对象列表，token 为 next token
    async fn list_objects(
        &self,
        bucket: String,
        prefix: Option<String>,
        max_keys: i32,
        continuation_token: Option<String>,
    ) -> Result<OssObjectsList>;

    //向文件添加对象列表，
    async fn append_object_list_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        continuation_token: Option<String>,
        file_path: String,
    ) -> Result<Option<String>>;

    // 按批次向文件添加所有描述的对象列表
    async fn append_all_object_list_to_file(
        &self,
        bucket: String,
        prefix: Option<String>,
        batch: i32,
        file_path: String,
    ) -> Result<()>;

    // 下载文件到目录
    async fn download_object_to_local(
        &self,
        bucket: String,
        key: String,
        dir: String,
    ) -> Result<()>;

    async fn download_objects_to_local(
        &self,
        bucket: String,
        keys: Vec<String>,
        dir: String,
    ) -> Result<()>;

    // 从本地传文件
    async fn upload_object_from_local(
        &self,
        bucket: String,
        key: String,
        file_path: String,
    ) -> Result<()>;

    // 获取 object 字节
    async fn get_object_bytes(&self, bucket: &str, key: &str) -> Result<Bytes>;

    // 上传 object 字节
    async fn upload_object_bytes(&self, bucket: &str, key: &str, content: Bytes) -> Result<()>;
}

// #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
// pub enum OssProvider {
//     JD,
//     JRSS,
//     ALI,
//     S3,
//     HUAWEI,
//     COS,
//     MINIO,
// }

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OssObjectsList {
    pub object_list: Option<Vec<String>>,
    pub next_token: Option<String>,
}

impl OSSDescription {
    #[allow(dead_code)]
    pub fn gen_oss_client_ref(&self) -> Result<Box<dyn OSSActions + Send + Sync>> {
        match self.provider {
            OssProvider::JD => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .build();

                let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                if let S3RequestStyle::PathStyle = self.request_style {
                    s3_config_builder = s3_config_builder.force_path_style(true);
                }
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let jdclient = OssJdClient { client };
                Ok(Box::new(jdclient))
            }

            OssProvider::S3 => {
                let shared_config = SdkConfig::builder()
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        self.access_key_id.clone(),
                        self.secret_access_key.clone(),
                        None,
                        None,
                        "Static",
                    )))
                    .endpoint_url(self.endpoint.clone())
                    .region(Region::new(self.region.clone()))
                    .build();

                let s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                let aws_client = OssJdClient { client };
                Ok(Box::new(aws_client))
            }
            OssProvider::ALI => todo!(),
            OssProvider::JRSS => todo!(),
            OssProvider::HUAWEI => todo!(),
            OssProvider::COS => todo!(),
            OssProvider::MINIO => todo!(),
        }
    }

    pub fn gen_oss_client(&self) -> Result<OssClient> {
        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(180)) // 连接建立超时
            .read_timeout(Duration::from_secs(180)) // 数据读取超时
            .build();
        let shared_config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                self.access_key_id.clone(),
                self.secret_access_key.clone(),
                None,
                None,
                "Static",
            )))
            .endpoint_url(self.endpoint.clone())
            .region(Region::new(self.region.clone()))
            .behavior_version(BehaviorVersion::latest())
            .timeout_config(timeout_config)
            .retry_config(RetryConfig::standard().with_max_attempts(5))
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .build();

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
        if let S3RequestStyle::PathStyle = self.request_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

        let oss_client = OssClient { client };

        match self.provider {
            OssProvider::JD => Ok(oss_client),
            OssProvider::ALI => Ok(oss_client),

            OssProvider::JRSS => Ok(oss_client),

            OssProvider::S3 => {
                // let shared_config = SdkConfig::builder()
                //     .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                //         self.access_key_id.clone(),
                //         self.secret_access_key.clone(),
                //         None,
                //         None,
                //         "Static",
                //     )))
                //     .endpoint_url(self.endpoint.clone())
                //     .region(Region::new(self.region.clone()))
                //     .behavior_version(BehaviorVersion::latest())
                //     .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
                //     .build();

                // let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);
                // if let S3RequestStyle::PathStyle = self.request_style {
                //     s3_config_builder = s3_config_builder.force_path_style(true);
                // }

                // let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
                // let oss_client = OssClient { client };
                Ok(oss_client)
            }
            OssProvider::HUAWEI => Ok(oss_client),

            OssProvider::COS => Ok(oss_client),

            OssProvider::MINIO => Ok(oss_client),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use tokio::{runtime, task::JoinSet};

    use crate::commons::read_yaml_file;

    use super::{OSSDescription, OssProvider};

    fn get_jd_oss_description() -> OSSDescription {
        let vec_oss = read_yaml_file::<Vec<OSSDescription>>("osscfg.yml").unwrap();
        let mut oss_jd = OSSDescription::default();
        for item in vec_oss.iter() {
            if item.provider == OssProvider::JD {
                oss_jd = item.clone();
            }
        }
        oss_jd
    }

    //cargo test s3::oss::test::test_ossaction_jd_append_all_object_list_to_file -- --nocapture
    #[test]
    fn test_ossaction_jd_append_all_object_list_to_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let oss_jd = get_jd_oss_description();
        let jd = oss_jd.gen_oss_client_ref();

        rt.block_on(async {
            let client = jd.unwrap();
            let r = client
                .append_all_object_list_to_file(
                    "jsw-bucket".to_string(),
                    None,
                    5,
                    "/tmp/jd_all_obj_list".to_string(),
                )
                .await;

            if let Err(e) = r {
                println!("{}", e.to_string());
                return;
            }
        });
    }

    //cargo test s3::oss::test::test_ossaction_jd_upload_object_form_file -- --nocapture
    #[test]
    fn test_ossaction_jd_upload_object_form_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let oss_jd = get_jd_oss_description();
        let jd = oss_jd.gen_oss_client_ref();

        rt.block_on(async {
            println!("upload");
            let client = jd.unwrap();
            let r = client
                .upload_object_from_local(
                    "jsw-bucket".to_string(),
                    "ali_download/cloud_game_new_arch.png".to_string(),
                    "/tmp/ali_download/cloud_game_new_arch.png".to_string(),
                )
                .await;

            if let Err(e) = r {
                println!("{}", e.to_string());
                return;
            }
        });
    }

    pub async fn sleep() {
        thread::sleep(Duration::from_secs(1));
    }

    //cargo test s3::oss::test::test_tokio_multi_thread -- --nocapture
    #[test]
    fn test_tokio_multi_thread() {
        let max_task = 2;
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(max_task)
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut set = JoinSet::new();
            for i in 0..100 {
                println!("run {}", i);
                while set.len() >= max_task {
                    set.join_next().await;
                }
                set.spawn(async move {
                    sleep().await;
                    println!("spawn {}", i);
                });
            }
            while set.len() >= max_task {
                set.join_next().await;
            }
        });
    }
}
