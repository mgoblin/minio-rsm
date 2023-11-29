/*
 * Copyright 2023 Michael Golovanov <mike.golovanov@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mg.kafka.tieredstorage.minio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Item;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;
import ru.mg.kafka.tieredstorage.minio.metadata.ByteEncodedMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO Add Javadoc
// TODO Update unit tests
// TODO Analyze CustomMetadata in deleteLogSegmentData
// TODO Add integration tests
// TODO Add README
public class NaiveRemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB
    private static final Logger log = LoggerFactory.getLogger(NaiveRemoteStorageManager.class);
    private static final String CONTENT_TYPE = "application/binary";

    private boolean initialized = false;

    private MinioClient minioClient;

    private ConnectionConfig config;

    /**
     * For testing
     * @param minioClient minioClient mock
     */
    NaiveRemoteStorageManager(final MinioClient minioClient) {
        Objects.requireNonNull(minioClient);
        this.minioClient = minioClient;
    }

    public NaiveRemoteStorageManager() {
        super();
    }

    private void ensureInitialized() {
        if (!initialized && config != null) {
            log.debug("Remote log manager not initialized. Try to initialize.");
            configure(config.originals());
            log.info(
                    "Remote log manager {} initialized now with {}",
                    NaiveRemoteStorageManager.class.getName(),
                    config.toString());
        }
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final LogSegmentData logSegmentData
    ) throws RemoteStorageException {
        log.trace("Start copy log segment data with metadata {} and segment data {}",
                remoteLogSegmentMetadata,
                logSegmentData);

        ensureInitialized();

        final var names = new NameAssigner(remoteLogSegmentMetadata);
        final var copyMetadata = new ByteEncodedMetadata();


        final var isDataCopied = copySegmentData(logSegmentData.logSegment(), names.logSegmentObjectName());
        copyMetadata.setDataNotEmpty(isDataCopied);

        final var isOffsetIndexCopied = copyOffsetIndex(logSegmentData.offsetIndex(), names.indexObjectName());
        copyMetadata.setIndexNotEmpty(isOffsetIndexCopied);

        final var isTimeIndexCopied = copyTimeIndex(logSegmentData.timeIndex(), names.timeIndexObjectName());
        copyMetadata.setTimeIndexNotEmpty(isTimeIndexCopied);

        if (logSegmentData.transactionIndex().isPresent()) {
            final var isTxnIndexCopied = copyTransactionalIndex(
                    logSegmentData.transactionIndex().get(),
                    names.transactionIndexObjectName());
            copyMetadata.setTransactionIndexNotEmpty(isTxnIndexCopied);
        } else {
            copyMetadata.setTransactionIndexNotEmpty(false);
            log.debug("Transactional index is empty, don't copy it");
        }

        final var isProducerSnapshotCopied = copyProducerSnapshotIndex(
                logSegmentData.producerSnapshotIndex(),
                names.producerSnapshotObjectName());
        copyMetadata.setProducerSnapshotIndexNotEmpty(isProducerSnapshotCopied);

        final var isLeaderEpochCopied = copyLeaderEpochIndex(
                logSegmentData.leaderEpochIndex(),
                names.leaderEpochObjectName());
        copyMetadata.setLeaderEpochIndexNotEmpty(isLeaderEpochCopied);

        final byte[] metadataBitmap = new byte[]{copyMetadata.getByteValue()};
        log.trace("Metadata bitmap is {} for {}", metadataBitmap, names.getBaseName());

        final var customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(metadataBitmap);

        log.trace("Copy log segment data with metadata {} and segment data {} finished",
                remoteLogSegmentMetadata,
                logSegmentData);

        return Optional.of(customMetadata);
    }

    private boolean copySegmentData(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "segment data");
        return true;
    }

    private boolean copyOffsetIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "offset index");
        return true;
    }

    private boolean copyTimeIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "time index");
        return true;
    }

    private boolean copyTransactionalIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "transactional index");
        return true;
    }

    private boolean copyProducerSnapshotIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "producer snapshot index");
        return true;
    }

    private boolean copyLeaderEpochIndex(final ByteBuffer data,  final String dstObjectName)
            throws RemoteStorageException {
        writeByteBufferToMinio(
                data,
                config.getMinioBucketName(),
                dstObjectName,
                "leader epoch index");
        return true;
    }

    private void writeFileByPathToMinio(
            final Path srcPath,
            final String bucketName,
            final String objectName,
            final String entityName
    ) throws RemoteStorageException {

        final var localFilePath = srcPath.normalize().toAbsolutePath();

        try {
            final var  bytes = FileUtils.readFileToByteArray(localFilePath.toFile());
            final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            writeByteBufferToMinio(byteBuffer, bucketName, objectName, entityName);
        } catch (final IOException e) {
            log.error("Access to file {} IO error", localFilePath, e);
            throw new RemoteStorageException(e);
        }
    }

    private void writeByteBufferToMinio(final ByteBuffer buffer,
                                                 final String bucketName,
                                                 final String objectName,
                                                 final String entityName
    ) throws RemoteStorageException {

        try (final var logSegmentInputStream = new ByteBufferInputStream(buffer)) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .contentType(CONTENT_TYPE)
                            .stream(logSegmentInputStream,
                                    logSegmentInputStream.available(),
                                    Math.max(logSegmentInputStream.available(), MIN_PART_SIZE))
                            .build()
            );
            log.debug("Copy {} to bucket {} with name {}", entityName, bucketName, objectName);
        } catch (final IOException e) {
            log.error("Minio S3 bucket {} IO operation on putObject {} failed.", bucketName, objectName, e);
            throw new RemoteStorageException(e);
        } catch (final ServerException e) {
            log.error("Minio S3 bucket {} server operation on putObject {} failed with http code {}.",
                    bucketName,
                    objectName,
                    e.statusCode(),
                    e);
            throw new RemoteStorageException(e);
        } catch (final InsufficientDataException e) {
            log.error(
                    "Minio S3 bucket {} operation on putObject {} failed. "
                    + "Not enough data available in InputStream.",
                    bucketName,
                    objectName,
                    e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on putObject {} failed. "
                            + "Error response with code {} and message {}.",
                    bucketName,
                    objectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | XmlParserException
                       | InternalException | InvalidResponseException e) {
            log.error(
                    "Minio S3 bucket {} operation on putObject {} failed. Internal minio error.",
                    bucketName,
                    objectName,
                    e);
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchLogSegment(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final int startPosition
    ) throws RemoteStorageException {
        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String segmentObjectName = nameAssigner.logSegmentObjectName();

        final byte metadataBitmap = remoteLogSegmentMetadata.customMetadata()
                .orElse(new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{0})).value()[0];

        final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata(metadataBitmap);
        final boolean isDataPresent = byteEncodedMetadata.isDataNotEmpty();

        log.debug("Fetch log segment from ref {} and start position {} with metadata bitmap {} started",
                segmentObjectName,
                startPosition,
                metadataBitmap);

        if (isDataPresent) {
            final byte[] content = fetchAllSegmentBytes(remoteLogSegmentMetadata);
            final byte[] subArray = Arrays.copyOfRange(content, startPosition, content.length);

            log.debug("Fetch log segment data from start position {} with path {} success. "
                            + "Fetched {} bytes, trimmed to {}.",
                    startPosition,
                    segmentObjectName,
                    content.length,
                    subArray.length);

            return new ByteArrayInputStream(subArray);
        } else {
            log.error("Fetch log segment data start position {} with path {} is cancelled.",
                    startPosition,
                    segmentObjectName);
            throw new RemoteResourceNotFoundException(String.format(
                    "Fetch segment %s with wrong metadata %s",
                    segmentObjectName,
                    metadataBitmap));
        }
    }

    @Override
    public InputStream fetchLogSegment(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final int startPosition,
            final int endPosition
    ) throws RemoteStorageException {
        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String segmentObjectName = nameAssigner.logSegmentObjectName();

        final byte metadataBitmap = remoteLogSegmentMetadata.customMetadata()
                .orElse(new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{0})).value()[0];

        final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata(metadataBitmap);
        final boolean isDataPresent = byteEncodedMetadata.isDataNotEmpty();

        if (isDataPresent) {
            log.debug("Fetch log segment from ref {}, start position {}, end position {} started.",
                    segmentObjectName,
                    startPosition,
                    endPosition);

            final byte[] content = fetchAllSegmentBytes(remoteLogSegmentMetadata);
            final byte[] subArray = Arrays.copyOfRange(content, startPosition, endPosition);

            log.debug("Fetch log segment data from start position {} with path {} success. "
                            + "Fetched {} bytes, trimmed to {}.",
                    startPosition,
                    segmentObjectName,
                    content.length,
                    subArray.length);

            return new ByteArrayInputStream(subArray);
        } else {
            log.error("Fetch log segment data start position {} end position {} with path {} is cancelled.",
                    startPosition,
                    endPosition,
                    segmentObjectName);
            throw new RemoteStorageException(String.format(
                    "Fetch segment %s with wrong metadata %s",
                    segmentObjectName,
                    metadataBitmap));
        }
    }

    private byte[] fetchAllSegmentBytes(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {
        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String segmentObjectName = nameAssigner.logSegmentObjectName();

        log.debug("Fetch log segment from ref {}", segmentObjectName);

        ensureInitialized();

        try {
            final var response = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(config.getMinioBucketName())
                            .object(segmentObjectName)
                            .build());

            final byte[] body = response.readAllBytes();

            log.debug("Fetch log segment data from path {} success. "
                            + "Fetched {} bytes.",
                    segmentObjectName,
                    body.length);

            return body;

        } catch (final IOException e) {
            log.error("Fetch log segment data from object {} failed. IO exception occurred.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final ServerException e) {
            log.error("Fetch log segment data from object {} failed. Http server error.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final InsufficientDataException e) {
            log.error("Fetch log segment data from object {} failed. Insufficient data.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. "
                            + "Error response with code {} and message {}.",
                    getBucketName(),
                    segmentObjectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException
                       | XmlParserException | InternalException | InvalidResponseException e) {
            log.error("Fetch log segment data from object {} failed. Internal error occurred.", segmentObjectName, e);
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public InputStream fetchIndex(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final IndexType indexType
    ) throws RemoteStorageException {

        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String indexObjectName = nameAssigner.indexNameByType(indexType);
        final var metadataBitmapOptional = remoteLogSegmentMetadata.customMetadata();

        if (metadataBitmapOptional.isEmpty()) {
            log.error("remoteLogSegmentMetadata.customMetadata is empty for index {} {}",
                    indexObjectName, indexType.name());
            throw new RemoteStorageException("remoteLogSegmentMetadata.customMetadata is empty");
        } else {
            final var metadataValue = remoteLogSegmentMetadata.customMetadata().get().value();

            if (metadataValue.length == 0) {
                log.error("remoteLogSegmentMetadata.customMetadata is value length is 0 for index {} {}",
                        indexObjectName, indexType.name());
                throw new RemoteStorageException("remoteLogSegmentMetadata.customMetadata is value length is 0");
            }

            final var metadataBitmap = metadataValue[0];
            log.debug("Fetch index {} from {} with bitmap {} started", indexType, indexObjectName, metadataBitmap);

            final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata(metadataBitmap);
            final boolean isIndexPresent = byteEncodedMetadata.isIndexOfTypePresent(indexType);

            if (isIndexPresent) {
                try {
                    final InputStream inputStream = readIndex(indexObjectName, indexType);
                    final var length = inputStream.available();
                    log.debug("Fetch index {} with path {} success. Fetched {} bytes.",
                            indexType,
                            indexObjectName,
                            length);

                    return inputStream;
                } catch (final IOException e) {
                    throw new RemoteResourceNotFoundException(e);
                }
            } else {
                if (indexType != IndexType.TRANSACTION) {
                    log.error("Fetch index {} from {} failed. Metadata doesn't have index flag {}",
                            indexType, indexObjectName, metadataBitmap);
                    throw new RemoteStorageException("Metadata flag for index is false");
                } else {
                    log.debug("Fetch index {} from {} finished. Log not found", indexType, indexObjectName);
                    throw new RemoteResourceNotFoundException("Transactional index is not found");
                }
            }
        }
    }

    private InputStream readIndex(final String indexObjectName, final IndexType indexType)
            throws RemoteStorageException {

        try {
            final var response = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(config.getMinioBucketName())
                            .object(indexObjectName)
                            .build());

            final byte[] body = response.readAllBytes();
            return new ByteArrayInputStream(body);
        } catch (final IOException e) {
            log.error("Fetch index {} from {} failed. IO exception occurred.", indexType, indexObjectName, e);
            throw new RemoteStorageException(e);
        } catch (final ServerException e) {
            log.error("Minio S3 bucket {} server operation on get {} failed with http code {}.",
                    getBucketName(),
                    indexObjectName,
                    e.statusCode(),
                    e);
            throw new RemoteStorageException(e);
        } catch (final InsufficientDataException e) {
            log.error(
                    "Minio S3 bucket {} operation on putObject {} failed. "
                            + "Not enough data available in InputStream.",
                    getBucketName(),
                    indexObjectName,
                    e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. "
                            + "Error response with code {} and message {}.",
                    getBucketName(),
                    indexObjectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                       | XmlParserException | InternalException e) {
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. Internal minio error.",
                    getBucketName(),
                    indexObjectName,
                    e);
            throw new RemoteStorageException(e);
        }
    }

    @Override
    public void deleteLogSegmentData(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata
    ) throws RemoteStorageException {
        log.trace("Delete log segment {}", remoteLogSegmentMetadata);

        ensureInitialized();

        final var names = new NameAssigner(remoteLogSegmentMetadata);

        log.debug("Delete log files {} started", names.getBaseName());

        final Collection<String> segmentObjectNames = List.of(
                names.logSegmentObjectName(),
                names.indexObjectName(),
                names.timeIndexObjectName(),
                names.transactionIndexObjectName(),
                names.producerSnapshotObjectName(),
                names.leaderEpochObjectName()
        );

        log.debug("Objects for delete from {} are {}", names.getBaseName(), segmentObjectNames);

        for (final String dataObjectName: segmentObjectNames) {
            try {
                minioClient.removeObject(
                        RemoveObjectArgs.builder()
                            .bucket(getBucketName())
                            .object(dataObjectName)
                            .build());
            } catch (final IOException | ServerException e) {
                log.error("Delete {} from {} error. IO or server exception occurred.",
                        dataObjectName,
                        getBucketName(),
                        e);
                throw new RemoteStorageException(e);
            } catch (final ErrorResponseException e) {
                if (!objectExists(dataObjectName)) {
                    break;
                } else {
                    final var errorCode = e.errorResponse().code();
                    final var errorMessage = e.errorResponse().message();
                    log.error(
                            "Minio S3 bucket {} operation on putObject {} failed. "
                                    + "Error response with code {} and message {}.",
                            getBucketName(),
                            dataObjectName,
                            errorCode,
                            errorMessage,
                            e);
                    throw new RemoteStorageException(e);
                }
            } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                           | XmlParserException | InsufficientDataException | InternalException e) {
                log.error("Delete {} from {} error. Internal server exception occurred.",
                        dataObjectName,
                        getBucketName(),
                        e);
                throw new RemoteStorageException(e);
            }
        }
        log.debug("Delete log files {} finished", names.getBaseName());
    }

    private boolean objectExists(final String dataObjectName) {
        final Iterable<Result<Item>> list = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(getBucketName())
                        .prefix(dataObjectName)
                        .build());
        return list.iterator().hasNext();
    }

    @Override
    public void close() {
        if (initialized) {
            minioClient = null;
            initialized = false;
        }
        log.debug("Remote storage manager closed");
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        log.debug("Configure remote storage manager");
        Objects.requireNonNull(configs, "configs must not be null");

        this.config = new ConnectionConfig(configs);

        if (minioClient == null) {
            minioClient = MinioClient.builder()
                    .endpoint(config.getMinioS3EndpointUrl())
                    .credentials(config.getMinioAccessKey(), config.getMinioSecretKey().value())
                    .build();
            log.debug("Minio client created");
        }

        try {
            if (makeBucketIfNotExists(config.getMinioBucketName(), config.isAutoCreateBucket())) {
                log.debug("Bucket {} created", config.getMinioBucketName());
            } else {
                log.debug("Bucket {} found", config.getMinioBucketName());
            }

            initialized = true;
            log.info(
                    "Remote log manager {} initialized with {}",
                    NaiveRemoteStorageManager.class.getName(),
                    config.toString());

        } catch (final IOException | ServerException | InternalException
                       | InsufficientDataException | ErrorResponseException e) {
            initialized = false;
            log.error("RemoteStorageManager configuration failed. recoverable error occurred.", e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                       | XmlParserException e) {
            log.error("Unrecoverable initialization error", e);
            initialized = false;
            throw new RuntimeException(e);
        }
    }

    private boolean makeBucketIfNotExists(
            final String bucketName,
            final boolean autoCreateBucketConfigured
    ) throws InvalidKeyException, NoSuchAlgorithmException, IOException, ServerException,
            InsufficientDataException, ErrorResponseException, InvalidResponseException,
            XmlParserException, InternalException {

        if (autoCreateBucketConfigured) {
            final boolean isBucketExists = minioClient.bucketExists(
                    BucketExistsArgs.builder().bucket(bucketName).build());


            if (!isBucketExists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }

            return !isBucketExists;
        } else {
            return false;
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public String getBucketName() {
        if (initialized) {
            return config.getMinioBucketName();
        } else {
            throw new IllegalArgumentException("Remote Storage Manager is not initialized");
        }
    }
}
