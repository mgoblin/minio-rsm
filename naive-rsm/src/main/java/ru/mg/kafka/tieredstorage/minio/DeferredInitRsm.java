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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Fetcher;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Uploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward Kafka RemoteStorageManager implementation with Minio S3
 * as tiered storage
 * <p>
 *     After creation by constructor, NaiveRemoteStorageManager is not fully initialized.
 *     The configure method is expected to be called with the configuration map to complete
 *     the initialization.
 * </p>
 * <p>
 *  *     Delegates writing Kafka log segments to Minio S3 to {@link Uploader} and fetching to
 *  * {@link Fetcher}.
 *  * </p>
 */
public class DeferredInitRsm
        extends NaiveRsm
        implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    /** Logger **/
    private static final Logger log = LoggerFactory.getLogger(DeferredInitRsm.class);

    /**
     * Ready to work flag.
     * <p>
     *     After the constructor is called, the flag is false, suggesting that the configure method will be called
     *     to complete the initialization.
     * </p>
     */
    private boolean initialized = false;

    private Map<String, ?> configs;

    // for testing
    void setBackend(final RemoteStorageBackend backend) {
        this.backend = backend;
    }

    public DeferredInitRsm() {
        super();
    }

    /**
     * Ensures that NaiveRemoteStorageManager is initialized and reinitializes if necessary.
     */
    public void ensureInitialized() throws RemoteStorageException {
        log.trace("Start ensuring {} initialized", DeferredInitRsm.class.getName());
        if (!initialized) {
            log.debug("Remote log manager not initialized. Try to initialize.");

            if (this.configs == null) {
                throw new RemoteStorageException("ensureInitialized should be called after config");
            }
            configure(this.configs);
            log.info(
                    "Remote log manager {} initialized now with {}",
                    DeferredInitRsm.class.getName(),
                    backend.getConfig().toString());
        }

        log.trace("Finish ensuring {} initialized", DeferredInitRsm.class.getName());
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
        ensureInitialized();
        return super.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
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
        ensureInitialized();
        return super.fetchLogSegment(remoteLogSegmentMetadata, startPosition);
    }

    /**
     * Fetches log segment data bytes from S3 Minio remote storage at [start .. end] position
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
        ensureInitialized();
        return super.fetchLogSegment(remoteLogSegmentMetadata, startPosition, endPosition);
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
        ensureInitialized();
        return super.fetchIndex(remoteLogSegmentMetadata, indexType);
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
        ensureInitialized();
        super.deleteLogSegmentData(remoteLogSegmentMetadata);
    }

    /**
     * Uninitialize and close RemoteStorageManager
     */
    @Override
    public void close() {
        log.trace("Closing RemoteStorageManager");
        if (initialized) {
            log.trace("Close initialized RemoteStorageManager.");
            super.close();
            initialized = false;
        } else {
            log.trace("RemoteStorageManager is uninitialized. Close do nothing.");
        }
        log.debug("Remote storage manager closed");
    }

    /**
     * Initialize RemoteStorageManager
     * @param configs user configs
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        log.trace("Staring to configure {}", DeferredInitRsm.class);

        Objects.requireNonNull(configs, "configs must not be null");
        if (this.configs == null) {
            this.configs = configs;
        }

        if (!initialized) {
            log.debug("Try to configure remote storage manager {}", DeferredInitRsm.class);

            initBackend(this.configs);

            log.debug("Backend {} configuration success.",
                    this.backend.getClass());
        }

        initialized = tryToMakeBucket(this.configs);
    }

    /**
     * Get initialization state
     * @return initialization state
     */
    @Override
    public boolean isInitialized() {
        if (initialized & !super.isInitialized()) {
            throw new IllegalStateException("RemoteStorageManager initialization state corrupted");
        }
        return initialized & super.isInitialized();
    }

    // for testing
    void setConfigs(final Map<String, ?> configs) {
        this.configs = configs;
    }

    /**
     * Get configs
     * @return configs
     */
    public Map<String, ?> getConfigs() {
        return new HashMap<>(configs);
    }

    // for testing
    void setInitialized(final boolean initialized) {
        this.initialized = initialized;
    }
}
