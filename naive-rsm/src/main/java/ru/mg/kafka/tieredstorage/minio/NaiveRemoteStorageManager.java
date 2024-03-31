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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import ru.mg.kafka.tieredstorage.minio.io.Fetcher;
import ru.mg.kafka.tieredstorage.minio.io.RecoverableConfigurationFailException;
import ru.mg.kafka.tieredstorage.minio.io.Writer;
import ru.mg.kafka.tieredstorage.minio.metadata.ByteEncodedMetadata;
import ru.mg.kafka.tieredstorage.minio.metadata.MetadataUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Add Javadoc
// TODO Update unit tests - add fixtures
// TODO Update unit tests - add parametric tests for exception cases
// TODO Add integration tests
public class NaiveRemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(NaiveRemoteStorageManager.class);

    private boolean initialized = false;

    private Writer ioWriter;
    private Fetcher ioFetcher;

    /**
     * For testing
     * @param ioWriter ioWriter mock
     * @param ioFetcher ioFetcher mock
     */
    NaiveRemoteStorageManager(final Writer ioWriter, final Fetcher ioFetcher) {
        this(ioWriter, ioFetcher, true);
    }

    private NaiveRemoteStorageManager(final Writer ioWriter, final Fetcher ioFetcher, final boolean initialized) {
        Objects.requireNonNull(ioWriter);
        Objects.requireNonNull(ioFetcher);
        this.ioWriter = ioWriter;
        this.ioFetcher = ioFetcher;
        this.initialized = initialized;
    }

    public NaiveRemoteStorageManager() {
        super();
    }

    private void ensureInitialized() {
        if (!initialized) {
            log.debug("Remote log manager not initialized. Try to initialize.");
            configure(ioFetcher.getConfig().originals());
            log.info(
                    "Remote log manager {} initialized now with {}",
                    NaiveRemoteStorageManager.class.getName(),
                    ioFetcher.getConfig().toString());
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


        final var isDataCopied = ioWriter.copySegmentData(
                logSegmentData.logSegment(),
                names.logSegmentObjectName());
        copyMetadata.setDataNotEmpty(isDataCopied);

        final var isOffsetIndexCopied = ioWriter.copyOffsetIndex(
                logSegmentData.offsetIndex(),
                names.indexObjectName());
        copyMetadata.setIndexNotEmpty(isOffsetIndexCopied);

        final var isTimeIndexCopied = ioWriter.copyTimeIndex(
                logSegmentData.timeIndex(),
                names.timeIndexObjectName());
        copyMetadata.setTimeIndexNotEmpty(isTimeIndexCopied);

        if (logSegmentData.transactionIndex().isPresent()) {
            final var isTxnIndexCopied = ioWriter.copyTransactionalIndex(
                    logSegmentData.transactionIndex().get(),
                    names.transactionIndexObjectName());
            copyMetadata.setTransactionIndexNotEmpty(isTxnIndexCopied);
        } else {
            copyMetadata.setTransactionIndexNotEmpty(false);
            log.debug("Transactional index is empty, don't copy it");
        }

        final var isProducerSnapshotCopied = ioWriter.copyProducerSnapshotIndex(
                logSegmentData.producerSnapshotIndex(),
                names.producerSnapshotObjectName());
        copyMetadata.setProducerSnapshotIndexNotEmpty(isProducerSnapshotCopied);

        final var isLeaderEpochCopied = ioWriter.copyLeaderEpochIndex(
                logSegmentData.leaderEpochIndex(),
                names.leaderEpochObjectName());
        copyMetadata.setLeaderEpochIndexNotEmpty(isLeaderEpochCopied);

        log.trace("Metadata bitmap is {} for {}", new byte[]{copyMetadata.getByteValue()}, names.getBaseName());
        final var customMetadata = MetadataUtils.customMetadata(copyMetadata.getByteValue());

        log.trace("Copy log segment data with metadata {} and segment data {} finished",
                remoteLogSegmentMetadata,
                logSegmentData);

        return Optional.of(customMetadata);
    }

    @Override
    public InputStream fetchLogSegment(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final int startPosition
    ) throws RemoteStorageException {
        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String segmentObjectName = nameAssigner.logSegmentObjectName();

        if (MetadataUtils.metadata(remoteLogSegmentMetadata).isDataNotEmpty()) {
            ensureInitialized();
            return ioFetcher.fetchLogSegmentData(segmentObjectName, startPosition);
        } else {
            log.error("Fetch log segment data start position {} with path {} is cancelled.",
                    startPosition,
                    segmentObjectName);
            throw new RemoteResourceNotFoundException(String.format(
                    "Fetch segment %s with wrong metadata %s",
                    segmentObjectName,
                    MetadataUtils.metadata(remoteLogSegmentMetadata)));
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

        if (MetadataUtils.metadata(remoteLogSegmentMetadata).isDataNotEmpty()) {
            ensureInitialized();
            return ioFetcher.fetchLogSegmentData(segmentObjectName, startPosition, endPosition);
        } else {
            log.error("Fetch log segment data start position {} end position {} with path {} is cancelled.",
                    startPosition,
                    endPosition,
                    segmentObjectName);
            throw new RemoteStorageException(String.format(
                    "Fetch segment %s with wrong metadata %s",
                    segmentObjectName,
                    MetadataUtils.metadata(remoteLogSegmentMetadata)));
        }
    }

    @Override
    public InputStream fetchIndex(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
            final IndexType indexType
    ) throws RemoteStorageException {
        final NameAssigner nameAssigner = new NameAssigner(remoteLogSegmentMetadata);
        final String indexObjectName = nameAssigner.indexNameByType(indexType);

        if (MetadataUtils.metadata(remoteLogSegmentMetadata).isIndexNotEmpty()) {
            try {
                ensureInitialized();
                final InputStream inputStream = ioFetcher.readIndex(indexObjectName, indexType);
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
                        indexType, indexObjectName, MetadataUtils.metadata(remoteLogSegmentMetadata));
                throw new RemoteStorageException("Metadata flag for index is false");
            } else {
                log.debug("Fetch index {} from {} finished. Log not found", indexType, indexObjectName);
                throw new RemoteResourceNotFoundException("Transactional index is not found");
            }
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
            ioFetcher.deleteSegmentObject(dataObjectName);
        }
        log.debug("Delete log files {} finished", names.getBaseName());
    }

    @Override
    public void close() {
        if (initialized) {
            ioWriter = null;
            ioFetcher = null;
            initialized = false;
        }
        log.debug("Remote storage manager closed");
    }

    @Override
    public void configure(final Map<String, ?> configs) {

        if (!initialized) {
            log.debug("Configure remote storage manager");
            Objects.requireNonNull(configs, "configs must not be null");

            this.ioWriter = new Writer(configs);
            this.ioFetcher = new Fetcher(configs);
        }

        try {
            ioWriter.makeBucketIfNotExists();

            initialized = true;
            log.info(
                    "Remote log manager {} initialized with {}",
                    NaiveRemoteStorageManager.class.getName(),
                    this.ioFetcher.getConfig().toString());

        } catch (final RecoverableConfigurationFailException e) {
            ioFetcher = null;
            ioWriter = null;
            initialized = false;
            log.error("RemoteStorageManager configuration failed. recoverable error occurred.", e);
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

}
