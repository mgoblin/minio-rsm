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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;
import ru.mg.kafka.tieredstorage.minio.metadata.ByteEncodedMetadata;

// TODO add Javadoc
// TODO Add README
// TODO add minio client all params support
// TODO update Unit tests
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
        if (!initialized) {
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

        try {
            final var names = new NameAssigner(remoteLogSegmentMetadata);
            final var copyMetadata = new ByteEncodedMetadata();


            final var isDataCopied = copySegmentData(logSegmentData.logSegment(), names.logSegmentObjectName());
            copyMetadata.setDataNotEmpty(isDataCopied);

            final var isOffsetIndexCopied = copyOffsetIndex(logSegmentData.offsetIndex(), names.indexObjectName());
            copyMetadata.setIndexNotEmpty(isOffsetIndexCopied);

            final var isTimeIndexCopied = copyTimeIndex(logSegmentData.timeIndex(), names.timeIndexObjectName());
            copyMetadata.setTimeIndexNotEmpty(isTimeIndexCopied);

            final var isTxnIndexCopied = copyTransactionalIndex(
                    logSegmentData.transactionIndex(),
                    names.transactionIndexObjectName());
            copyMetadata.setTransactionIndexNotEmpty(isTxnIndexCopied);

            final var isProducerSnapshotCopied = copyProducerSnapshotIndex(
                    logSegmentData.producerSnapshotIndex(),
                    names.producerSnapshotIndexObjectName());
            copyMetadata.setProducerSnapshotIndexNotEmpty(isProducerSnapshotCopied);

            final var isLeaderEpochCopied = copyLeaderEpochIndex(
                    logSegmentData.leaderEpochIndex(),
                    names.leaderEpochIndexObjectName());
            copyMetadata.setLeaderEpochIndexNotEmpty(isLeaderEpochCopied);

            final byte[] metadataBitmap = new byte[]{copyMetadata.getValue()};
            log.trace("Metadata bitmap is {} for {}", metadataBitmap, names.getBaseName());

            final var customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(metadataBitmap);

            log.trace("Copy log segment data with metadata {} and segment data {} finished",
                    remoteLogSegmentMetadata,
                    logSegmentData);

            return Optional.of(customMetadata);
        } catch (MinioException | InvalidKeyException | IOException | NoSuchAlgorithmException e) {
            log.error("Copy log segment data with metadata {} and segment data {} failed.",
                    remoteLogSegmentMetadata,
                    logSegmentData,
                    e);
            throw new RemoteStorageException(e);
        }
    }

    private boolean copySegmentData(final Path srcPath, final String dstObjectName)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
        return writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "segment data") != null;
    }

    private boolean copyOffsetIndex(final Path srcPath, final String dstObjectName)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
        return writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "offset index") != null;
    }

    private boolean copyTimeIndex(final Path srcPath, final String dstObjectName)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
        return writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "time index") != null;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private boolean copyTransactionalIndex(final Optional<Path> maybeSrcPath, final String dstObjectName)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
        if (maybeSrcPath.isPresent()) {
            return writeFileByPathToMinio(
                    maybeSrcPath.get(),
                    config.getMinioBucketName(),
                    dstObjectName,
                    "transactional index") != null;
        } else {
            log.debug("Transactional index is empty, dont write it");
            return false;
        }
    }

    private boolean copyProducerSnapshotIndex(final Path srcPath, final String dstObjectName)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
        return writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "producer snapshot index") != null;
    }

    private boolean copyLeaderEpochIndex(final ByteBuffer data,  final String dstObjectName)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {
        return writeByteBufferToMinio(
                data,
                config.getMinioBucketName(),
                dstObjectName,
                "leader epoch index") != null;
    }

    private ObjectWriteResponse writeFileByPathToMinio(final Path srcPath,
                                                       final String bucketName,
                                                       final String objectName,
                                                       final String entityName
    ) throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {

        final var localFilePath = srcPath.normalize().toAbsolutePath();

        final var bytes = FileUtils.readFileToByteArray(localFilePath.toFile());
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        return writeByteBufferToMinio(byteBuffer, bucketName, objectName, entityName);
    }

    private ObjectWriteResponse writeByteBufferToMinio(final ByteBuffer buffer,
                                                 final String bucketName,
                                                 final String objectName,
                                                 final String entityName
    ) throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {

        try (final var logSegmentInputStream = new ByteBufferInputStream(buffer)) {
            final ObjectWriteResponse objectWriteResponse = minioClient.putObject(
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
            return objectWriteResponse;
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
            throw new RemoteStorageException(String.format(
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

        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            log.error("Fetch log segment data from path {} failed.", segmentObjectName, e);
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
        final byte allIndexesBitmap = (byte) 0xff;
        final byte metadataBitmap = remoteLogSegmentMetadata.customMetadata()
                .orElse(new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{allIndexesBitmap})).value()[0];

        log.debug("Fetch index {} from {} with bitmap {} started", indexType, indexObjectName, metadataBitmap);

        final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata(metadataBitmap);
        final boolean isIndexPresent = byteEncodedMetadata.isIndexOfTypePresent(indexType);

        try {
            return readIndex(isIndexPresent, indexObjectName, indexType);
        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            log.error("Fetch index {} from {} failed.", indexType, indexObjectName, e);
            throw new RemoteStorageException(e);
        }
    }

    private InputStream readIndex(
            final boolean isIndexPresent,
            final String indexObjectName,
            final IndexType indexType)
            throws MinioException, IOException, InvalidKeyException, NoSuchAlgorithmException {

        if (isIndexPresent) {
            final var response = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(config.getMinioBucketName())
                            .object(indexObjectName)
                            .build());

            final byte[] body = response.readAllBytes();

            log.debug("Fetch index {} with path {} success. Fetched {} bytes.",
                    indexType,
                    indexObjectName,
                    body.length);

            return new ByteArrayInputStream(body);
        } else {
            log.debug("Fetch index {} with path {} masked by custom metadata.",
                    indexType,
                    indexObjectName);
            return InputStream.nullInputStream();
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

        final List<String> segmentObjectNames = List.of(
                names.logSegmentObjectName(),
                names.indexObjectName(),
                names.timeIndexObjectName(),
                names.transactionIndexObjectName(),
                names.producerSnapshotIndexObjectName(),
                names.leaderEpochIndexObjectName()
        );
        final var  deleteObjects = segmentObjectNames.stream()
                .map(DeleteObject::new)
                .collect(Collectors.toList());

        log.debug("Objects for delete from {} are {}", names.getBaseName(), segmentObjectNames);

        final var results = minioClient.removeObjects(
                RemoveObjectsArgs.builder()
                        .bucket(config.getMinioBucketName())
                        .objects(deleteObjects)
                        .build());

        try {
            for (final Result<DeleteError> result : results) {
                final DeleteError error = result.get();
                log.error("Delete object error {} {}", error.objectName(), error.message());
            }
        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            log.error("Delete {} segment error", names.getBaseName(), e);
            throw new RemoteStorageException(e);
        }
        log.debug("Delete log files {} finished", names.getBaseName());
    }

    @Override
    public void close() {
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

        } catch (MinioException | InvalidKeyException | IOException | NoSuchAlgorithmException e) {
            initialized = false;
            log.error("RemoteStorageManager configuration failed", e);
        }
    }

    private boolean makeBucketIfNotExists(
            final String bucketName,
            final boolean autoCreateBucketConfigured
    ) throws MinioException, InvalidKeyException, NoSuchAlgorithmException, IOException {
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
