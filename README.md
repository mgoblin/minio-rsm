# Tiered Storage for Apache Kafka and Minio
This project is an implementation of RemoteStorageManager for Apache Kafka tiered storage.
The Implementation is supports only Minio as S3 backend and recommended only for
demonstrating technical architecture ideas/testing S3 tiered storage objectives.

The project follows the API specifications according to the latest version of 
[KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)

## Design

### Requirements
This implementation was done with few additional requirements:
* Supports for Minio only, preferably on-premise setup 
* Code structure should be as simple as possible with minimal abstractions and dependencies
* Supports latest LTS JDK release (JDK 21)
* Supports latest Apache Kafka 3.7

### Design details
NaiveRemoteStorageManager is a Kafka Remote Storage Manager implementation.
RemoteStorageManager is a simple interface with responsibilities:
1. Copy segment data and indexes from Kafka broker local files to remote storage
2. Fetch segment data with offset ranges from remote storage
3. Delete segment data and indexes from remote storage

NaiveRemoteStorageManager delegates interaction with S3 to backend (RemoteStorageBackend class).
Backend consists of uploader, fetcher, deleter and bucket. Each part responsible for 
part of storage interaction.

Currently, the loading, fetching and deleting tools use only the basic capabilities of the minio SDK, and not
optimized for code clarity.


## Getting started
### How to build code
```bash
./gradlew clean build
```
Result will be placed to zip and tar.gz archives in the 
./naive-rsm/build/distributions folder

### Kafka and Minio setup
1. Unzip/untar files from ./naive-rsm/build/distributions to <Kafka folder>/lib
2. Install and start Minio server 
```bash
minio server --address localhost:9000 <path to data folder>
```
3. Configure brokers (add following lines to kafka server.properties file)
```properties
############################# Kafka Tiered storage Settings #################################

# ----- Enable tiered storage -----
remote.log.storage.system.enable=true

# ----- Configure the remote log manager -----
# This is the default, but adding it for explicitness:
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager

# Put the real listener name you'd like to you here:
remote.log.metadata.manager.listener.name=PLAINTEXT

## For one node cluster
rlmm.config.remote.log.metadata.topic.replication.factor=1
## Only for testing
rlmm.config.remote.log.metadata.topic.num.partitions=1

remote.log.storage.manager.class.name=ru.mg.kafka.tieredstorage.minio.NaiveRemoteStorageManager
remote.log.storage.manager.impl.prefix=rsm.config.
rsm.config.minio.url=http://127.0.0.1:9000
rsm.config.minio.access.key=<access key - minioadmin>
rsm.config.minio.secret.key=<secret key - minio admin>
rsm.config.minio.auto.create.bucket=true
```
You should change access key and secret key by your values. 
4. Start Kafka cluster and check server.log and rsm.log doesn't have error lines.
5. Check "kafka-tiered-storage-bucket" bucket created on minio server  
6. Create topic with remote storage support
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic topic1 \
    --config remote.storage.enable=true \
    --config segment.bytes=512000 \
    --config local.retention.bytes=1 \
    --config retention.bytes=5120000
```
6. publish messages and after some time you will start seeing segments on 
the minio bucket, and later they will be deleted locally
```bash
   bin/kafka-producer-perf-test.sh \
   --topic topic1 --num-records=10000 --throughput -1 --record-size 1000 \
   --producer-props acks=1 batch.size=16384 bootstrap.servers=localhost:9092
```
7. Read all messages 
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic topic1 --from-beginning --timeout-ms 1000
```
You will see all messages (from local files and remote storage)

## License
The project is licensed under the Apache license, version 2.0. Full license text is available 
in the [LICENSE](LICENSE) file.