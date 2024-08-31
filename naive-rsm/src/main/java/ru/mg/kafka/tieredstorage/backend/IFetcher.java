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

package ru.mg.kafka.tieredstorage.backend;

import java.io.InputStream;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

/**
 * S3 fetcher interface. Fetches log segment data and indexes from S3 remote storage
 */
public interface IFetcher {

    /**
     * Fetches log segment data from S3 storage from start to end position.
     *
     * @param segmentObjectName - object name in S3 remote storage
     * @param startPosition start position
     * @param endPosition end position
     * @return Input stream with segment data
     * @throws RemoteStorageException on error
     */
    InputStream fetchLogSegmentData(
            final String segmentObjectName,
            final int startPosition,
            final int endPosition) throws RemoteStorageException;

    /**
     * Fetches index data from remote S3 storage.
     *
     * @param indexObjectName object name
     * @param indexType index type
     * @return Input stream with index data
     * @throws RemoteStorageException on error
     */
    InputStream readIndex(
            final String indexObjectName,
            final RemoteStorageManager.IndexType indexType)
            throws RemoteStorageException;
}
