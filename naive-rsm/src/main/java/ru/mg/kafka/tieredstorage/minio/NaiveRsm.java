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

import java.io.InputStream;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend;
import ru.mg.kafka.tieredstorage.metadata.ByteEncodedMetadata;
import ru.mg.kafka.tieredstorage.metadata.MetadataUtils;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Fetcher;
import ru.mg.kafka.tieredstorage.minio.backend.naive.MinioS3Backend;
import ru.mg.kafka.tieredstorage.minio.backend.naive.RecoverableConfigurationFailException;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Uploader;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;
import ru.mg.kafka.tieredstorage.naming.NameAssigner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Add integration tests
/**
 * Straightforward Kafka RemoteStorageManager implementation with Minio S3
 * as tiered storage
 * <p>
 *     After creation by constructor, NaiveRsm is not fully initialized.
 *     The configure method is expected to be called with the configuration map to complete
 *     the initialization.
 * </p>
 * <p>
 *  *     Delegates writing Kafka log segments to Minio S3 to {@link Uploader} and fetching to
 *  * {@link Fetcher}.
 *  * </p>
 */
public class NaiveRsm implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    /** Logger **/
    private static final Logger log = LoggerFactory.getLogger(NaiveRsm.class);

    /** Backend for minio interactions */
    protected RemoteStorageBackend backend;

    /**
     * For testing purposes
     * @param backend backend mock
     */
    NaiveRsm(final RemoteStorageBackend backend) {
        this.backend = Objects.requireNonNull(backend);
    }

    public NaiveRsm() {
        super();
    }

    /**
     * Copies log segment data and indexes to S3 Minio remote storage.
     *
     * @param remoteLogSegmentMetadata metadata
     * @param logSegmentData log segment data
     * @return custom metadata that contains copied indexes bimap
     * @throws RemoteStorageException on failures
     */
    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final LogSegmentData logSegmentData
    ) throws RemoteStorageException {
        log.trace("Start copy log segment data with metadata {} and segment data {}",
                remoteLogSegmentMetadata,
                logSegmentData);

        final var names = new NameAssigner(remoteLogSegmentMetadata);
        final var copyMetadata = new ByteEncodedMetadata();


        backend.uploader().copySegmentData(logSegmentData.logSegment(), names.logSegmentObjectName());
        copyMetadata.setDataNotEmpty(true);

        backend.uploader().copyOffsetIndex(logSegmentData.offsetIndex(), names.indexObjectName());
        copyMetadata.setIndexNotEmpty(true);

        backend.uploader().copyTimeIndex(logSegmentData.timeIndex(), names.timeIndexObjectName());
        copyMetadata.setTimeIndexNotEmpty(true);

        if (logSegmentData.transactionIndex().isPresent()) {
            backend.uploader().copyTransactionalIndex(
                    logSegmentData.transactionIndex().get(),
                    names.transactionIndexObjectName());
            copyMetadata.setTransactionIndexNotEmpty(true);
        } else {
            copyMetadata.setTransactionIndexNotEmpty(false);
            log.debug("Transactional index is empty, don't copy it");
        }

        backend.uploader().copyProducerSnapshotIndex(
                logSegmentData.producerSnapshotIndex(),
                names.producerSnapshotObjectName());
        copyMetadata.setProducerSnapshotIndexNotEmpty(true);

        backend.uploader().copyLeaderEpochIndex(logSegmentData.leaderEpochIndex(), names.leaderEpochObjectName());
        copyMetadata.setLeaderEpochIndexNotEmpty(true);

        log.trace("Metadata bitmap is {} for {}", new byte[]{copyMetadata.getByteValue()}, names.getBaseName());
        final var customMetadata = MetadataUtils.customMetadata(copyMetadata.getByteValue());

        log.trace("Copy log segment data with metadata {} and segment data {} finished",
                remoteLogSegmentMetadata,
                logSegmentData);

        return Optional.of(customMetadata);
    }

    /**
     * Fetches log segment data bytes from S3 Minio remote storage at start position
     *
     * @param remoteLogSegmentMetadata segment metadata
     * @param startPosition start position
     * @return segment data input stream
     * @throws RemoteStorageException on failures
     */
    @Override
    public InputStream fetchLogSegment(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final int startPosition
    ) throws RemoteStorageException {
        log.trace("Start to fetching log segment with metadata {} and segment from start position {}",
                remoteLogSegmentMetadata,
                startPosition);
        final int endPosition = remoteLogSegmentMetadata.segmentSizeInBytes() - 1;
        return fetchLogSegment(remoteLogSegmentMetadata, startPosition, endPosition);
    }

    /**
     * Fetches log segment data bytes from S3 Minio remote storage at [start, end] position
     *
     * @param remoteLogSegmentMetadata segment metadata
     * @param startPosition start position
     * @param endPosition end position
     * @return segment data input stream
     * @throws RemoteStorageException on failures
     */
    @Override
    public InputStream fetchLogSegment(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final int startPosition,
            final int endPosition
    ) throws RemoteStorageException {
        log.trace("Start to fetching log segment with metadata {} and segment from start {} to end {} position.",
                remoteLogSegmentMetadata,
                startPosition,
                endPosition
        );

        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String segmentObjectName = nameAssigner.logSegmentObjectName();

        if (MetadataUtils.metadata(remoteLogSegmentMetadata).isDataNotEmpty()) {
            log.trace("Fetch from start {} to end {} metadata flag is set for log data", startPosition, endPosition);
            final InputStream inputStream = backend.fetcher().fetchLogSegmentData(
                    segmentObjectName,
                    startPosition,
                    endPosition);
            log.trace("Fetch log segment with metadata {} and segment from start {} to end {} position finished.",
                    remoteLogSegmentMetadata,
                    startPosition,
                    endPosition);
            return inputStream;
        } else {
            log.error("Wrong metadata flag for segment. "
                            + "Fetch log segment data start {} to end {} position  with path {} is cancelled.",
                    startPosition,
                    endPosition,
                    segmentObjectName);
            throw new RemoteResourceNotFoundException(String.format(
                    "Fetch segment %s have empty data exists metadata flag %s",
                    segmentObjectName,
                    MetadataUtils.metadata(remoteLogSegmentMetadata)));
        }
    }

    /**
     * Fetch index if exists from S3 Minio remote storage
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @param indexType                type of the index to be fetched for the segment.
     * @return input stream with index data
     * @throws RemoteStorageException on failure
     */
    @Override
    public InputStream fetchIndex(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final IndexType indexType
    ) throws RemoteStorageException {

        log.trace("Start to fetch index with type {} and metadata {}", indexType, remoteLogSegmentMetadata);

        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String indexObjectName = nameAssigner.indexNameByType(indexType);

        final var metadata = MetadataUtils.metadata(remoteLogSegmentMetadata);
        final byte metadataBitmap = metadata.getByteValue();
        log.debug("Metadata bitmap value for index {} is {}", indexType, metadataBitmap);

        if (metadata.isIndexOfTypePresent(indexType)) {
            log.trace("Fetch index {} metadata flag is set", indexType);
            final InputStream inputStream = backend.fetcher().readIndex(indexObjectName, indexType);
            log.trace("Fetch index with type {} and metadata {} finished", indexType, remoteLogSegmentMetadata);
            return inputStream;
        } else {
            log.debug("Fetch index {} from {} finished. Index have empty metadata flag", indexType, indexObjectName);
            throw new RemoteResourceNotFoundException(String.format(
                    "Index %s for %s is not found because have empty metadata flag", indexType, indexObjectName));
        }
    }

    /**
     * Delete log segment data and indexes from S3 Minio remote storage.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment to be deleted.
     * @throws RemoteStorageException on failure
     */
    @Override
    public void deleteLogSegmentData(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata
    ) throws RemoteStorageException {
        log.trace("Delete log segment {}", remoteLogSegmentMetadata);

        final List<String> segmentObjectNames = objectNamesForDeletion(remoteLogSegmentMetadata);

        log.debug("Objects for delete are {}",
                String.join(", " + System.lineSeparator(), segmentObjectNames));

        for (final String objectName: segmentObjectNames) {
            log.trace("Delete {} object", objectName);
            backend.deleter().deleteSegmentObject(objectName);
        }

        log.debug("Delete log files {} finished",
                String.join(", " + System.lineSeparator(), segmentObjectNames));
    }

    private List<String> objectNamesForDeletion(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        final var names = new NameAssigner(remoteLogSegmentMetadata);

        final ByteEncodedMetadata metadata = MetadataUtils.metadata(remoteLogSegmentMetadata);

        final Map<String, Boolean> namesWithMetadata = Map.of(
                names.logSegmentObjectName(), metadata.isDataNotEmpty(),
                names.indexObjectName(), metadata.isIndexNotEmpty(),
                names.timeIndexObjectName(), metadata.isTimeIndexNotEmpty(),
                names.transactionIndexObjectName(), metadata.isTransactionIndexNotEmpty(),
                names.producerSnapshotObjectName(), metadata.isProducerSnapshotIndexNotEmpty(),
                names.leaderEpochObjectName(), metadata.isLeaderEpochIndexNotEmpty()
        );

        return namesWithMetadata.entrySet().stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * Close RemoteStorageManager
     */
    @Override
    public void close() {
        backend = null;
        log.debug("Remote storage manager closed");
    }

    /**
     * Initialize Remote Storage Manager
     * @param configs user configs
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        log.trace("Staring to configure {}", this.getClass());

        initBackend(Objects.requireNonNull(configs));
        if (!tryToMakeBucket(Objects.requireNonNull(configs))) {
            backend = null;
        }
    }


    /**
     * Create backend
     * @param configs RSM configs
     */
    protected void initBackend(final Map<String, ?> configs) {
        if (backend == null) {
            this.backend = new MinioS3Backend(Objects.requireNonNull(configs, "configs must not be null"));
        }
    }

    /**
     * Try to create bucket if not exists and config minio.auto.create.bucket = true
     * @param configs RSM configs
     */
    protected boolean tryToMakeBucket(final Map<String, ?> configs) {
        try {
            backend.bucket().tryToMakeBucket();

            log.info(
                    "Remote log manager {} initialized with {}",
                    this.getClass().getName(),
                    new ConnectionConfig(configs).originals());

            return true;
        } catch (final RecoverableConfigurationFailException e) {
            log.error("{} configuration failed. recoverable error occurred.",
                    this.getClass(),
                    e);
            return false;
        }
    }

    /**
     * Get initialization state
     * @return initialization state
     */
    public boolean isInitialized() {
        return backend != null;
    }
}
