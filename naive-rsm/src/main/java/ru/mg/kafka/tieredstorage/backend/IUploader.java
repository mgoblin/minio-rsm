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

import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

/**
 * S3 uploader interface. Upload data and indexes to remote S3 storage.
 */
public interface IUploader {

    /**
     * Uploads segment segment data from local storage to remote S3 storage.
     *
     * @param srcPath path to source local segment file
     * @param dstObjectName S3 remote storage destination objectName
     * @return true on success
     * @throws RemoteStorageException on error
     */
    boolean copySegmentData(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    /**
     * Uploads offset index data from local storage to remote S3 storage.
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @return true on success
     * @throws RemoteStorageException on error
     */
    boolean copyOffsetIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    /**
     * Uploads time index data from local storage to remote S3 storage.
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @return true on success
     * @throws RemoteStorageException on error
     */
    boolean copyTimeIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    /**
     * Uploads transactional index data from local storage to remote S3 storage.
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @return true on success
     * @throws RemoteStorageException on error
     */
    boolean copyTransactionalIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    /**
     * Uploads producer snapshot index data from local storage to remote S3 storage.
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @return true on success
     * @throws RemoteStorageException on error
     */
    boolean copyProducerSnapshotIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    /**
     * Uploads leader epoch index data from local storage to remote S3 storage.
     *
     * @param data index data byte buffer
     * @param dstObjectName S3 remote storage destination objectName
     * @return true on success
     * @throws RemoteStorageException on error
     */
    boolean copyLeaderEpochIndex(
            final ByteBuffer data,
            final String dstObjectName)
            throws RemoteStorageException;
}
