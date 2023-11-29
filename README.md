# Tiered Storage for Apache Kafka and Minio
This project is an implementation of RemoteStorageManager for Apache Kafka tiered storage.
The Implementation is supports only Minio as S3 backend and recommended only for
demonstrating technical architecture ideas/testing S3 tiered storage objectives.

The project follows the API specifications according to the latest version of 
[KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)

## Design

### Requirements
This implementation was done with few additional requirements:
* Support for Minio only, preferably on-premise setup 
* Code structure should be as simple as possible with minimal abstractions and dependencies

### Design details

## Getting started
TBD

## License
The project is licensed under the Apache license, version 2.0. Full license test is available 
in the [LICENSE](LICENSE) file.