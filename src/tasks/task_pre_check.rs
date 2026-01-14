use crate::models::model_task::ObjectStorage;
use anyhow::{Context, Result};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
use uuid::Uuid;

/// pre check 用于任务校验
/// 校验项包括，源、目标是否存在，及是否有相应权限

impl ObjectStorage {
    pub async fn storage_exists(&self) -> Result<bool> {
        match self {
            //本地路径判断路径是否存在
            ObjectStorage::Local(l) => {
                let path = Path::new(l);
                return Ok(path.exists());
            }
            //对象存储，判断 bucket 是否存在
            ObjectStorage::OSS(ossdescription) => {
                let client = ossdescription.gen_oss_client()?.client;
                client
                    .head_bucket()
                    .bucket(&ossdescription.bucket)
                    .send()
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;
                return Ok(true);
            }
        }
    }

    pub async fn has_read_permission(&self) -> Result<bool> {
        match self {
            ObjectStorage::Local(l) => {
                let metadata = fs::metadata(l)?;
                let permissions = metadata.permissions();
                let mode = permissions.mode();

                // 检查所有者、组、其他是否有读权限（分别对应 0o400、0o040、0o004）
                Ok((mode & 0o444) != 0)
            }
            ObjectStorage::OSS(ossdescription) => {
                let client = ossdescription.gen_oss_client()?.client;
                let _ = client
                    .list_objects_v2()
                    .bucket(&ossdescription.bucket)
                    .send()
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;
                Ok(true)
            }
        }
    }

    pub async fn has_write_permission(&self) -> Result<bool> {
        match self {
            ObjectStorage::Local(l) => {
                let metadata = fs::metadata(l)?;
                let permissions = metadata.permissions();
                let mode = permissions.mode();

                // 检查所有者、组、其他是否有写权限（分别对应 0o200、0o020、0o002）
                Ok((mode & 0o222) != 0)
            }
            ObjectStorage::OSS(ossdescription) => {
                let client = ossdescription.gen_oss_client()?.client;
                let test_key = format!("write_test_{}", Uuid::new_v4());

                // 尝试上传空对象
                let _ = client
                    .put_object()
                    .bucket(&ossdescription.bucket)
                    .key(&test_key)
                    .body(aws_sdk_s3::primitives::ByteStream::from_static(b""))
                    .send()
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;

                let _ = client
                    .delete_object()
                    .bucket(&ossdescription.bucket)
                    .key(&test_key)
                    .send()
                    .await
                    .context(format!("{}:{}", file!(), line!()))?;
                Ok(true)
            }
        }
    }

    pub async fn oss_clean_multi_parts(&self) -> Result<()> {
        if let ObjectStorage::OSS(ossdescription) = self {
            let client =
                ossdescription
                    .gen_oss_client()
                    .context(format!("{}:{}", file!(), line!()))?;
            client
                .clean_multi_part(&ossdescription.bucket)
                .await
                .context(format!("{}:{}", file!(), line!()))?;
        }
        Ok(())
    }
}
