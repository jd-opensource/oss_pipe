# hdfs 迁移方案

## hdfs 搭建
hadoop 下载地址 https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

### 配置 hdfs
sbin/start-dfs.sh
```
#!/usr/bin/env bash
HDFS_DATANODE_USER=root
HADOOP_SECURE_DN_USER=root
HDFS_NAMENODE_USER=root
HDFS_SECONDARYNAMENODE_USER=root
```

sbin/stop-dfs.sh
```
#!/usr/bin/env bash
HDFS_DATANODE_USER=root
HADOOP_SECURE_DN_USER=root
HDFS_NAMENODE_USER=root
HDFS_SECONDARYNAMENODE_USER=root
```

etc/hadoop/hadoop-env.sh
```
export JAVA_HOME=$HOME/amazon-corretto-21.0.3.9.1-linux-x64
```

etc/hadoop/core-site.xml
```
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://node-master:8020</value>
    </property>
</configuration>
```

etc/hadoop/hdfs-site.xml
```
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/root/hadoop/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/root/hadoop/dfs/data</value>
    </property>
    <property>
        <name>dfs.namenode.checkpoint.dir</name>
        <value>/root/hadoop/dfs/secondary</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
     </property>
</configuration>
```

### 启动 hdfs

```
bin/hdfs namenode -format
sbin/start-dfs.sh
```

### 验证
```
bin/hdfs dfs -ls /
```

## Alluxio 与 hdfs 集成

下载地址 https://www.alluxio.io/download/

### 配置 Alluxio 
 
```
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

conf/alluxio-site.properties
```
alluxio.master.mount.table.root.ufs=hdfs://node-master:8020
alluxio.underfs.hdfs.configuration=/root/hadoop-3.3.6/etc/hadoop/core-site.xml:/root/hadoop-3.3.6/etc/hadoop/h
dfs-site.xml
```


### 启动

./bin/alluxio format

./bin/alluxio validateEnv local

```
./bin/alluxio-start.sh local SudoMount
```
./bin/alluxio runTests

integration/fuse/bin/alluxio-fuse mount /mnt/alluxio-fuse /


## 利用 oss_pipe 迁移 hdfs 上的文件