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

public interface IUploader {

    boolean copySegmentData(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    boolean copyOffsetIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    boolean copyTimeIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    boolean copyTransactionalIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    boolean copyProducerSnapshotIndex(
            final Path srcPath,
            final String dstObjectName)
            throws RemoteStorageException;

    boolean copyLeaderEpochIndex(
            final ByteBuffer data,
            final String dstObjectName)
            throws RemoteStorageException;
}
