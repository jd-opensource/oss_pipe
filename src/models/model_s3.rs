use aws_sdk_s3::{types::Object, Client};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct ObjectRange {
    pub part_num: i32,
    pub begin: usize,
    pub end: usize,
}

//Todo 尝试修改为 Arc::<Client>
#[derive(Debug, Clone)]
pub struct OssClient {
    pub client: Client,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OssObjList {
    pub object_list: Option<Vec<Object>>,
    pub next_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum OssProvider {
    JD,
    JRSS,
    ALI,
    S3,
    HUAWEI,
    COS,
    MINIO,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum S3RequestStyle {
    PathStyle,
    VirtualHostedStyle,
}

impl Default for S3RequestStyle {
    fn default() -> Self {
        S3RequestStyle::VirtualHostedStyle
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct OssObjectsList {
    pub object_list: Option<Vec<String>>,
    pub next_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OSSDescription {
    pub provider: OssProvider,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    #[serde(default = "OSSDescription::prefix_default")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default = "OSSDescription::s3requeststyle")]
    pub request_style: S3RequestStyle,
}

impl Default for OSSDescription {
    fn default() -> Self {
        Self {
            provider: OssProvider::JD,
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            endpoint: "http://s3.cn-north-1.jdcloud-oss.com".to_string(),
            region: "cn-north-1".to_string(),
            bucket: "bucket_name".to_string(),
            prefix: Some("test/samples/".to_string()),
            request_style: S3RequestStyle::default(),
        }
    }
}

impl OSSDescription {
    fn prefix_default() -> Option<String> {
        None
    }

    fn s3requeststyle() -> S3RequestStyle {
        S3RequestStyle::VirtualHostedStyle
    }
}
