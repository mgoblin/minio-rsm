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

package ru.mg.kafka.tieredstorage.minio.backend.naive;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.io.FileUtils;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import ru.mg.kafka.tieredstorage.backend.IUploader;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploader implementation for Minio S3
 *
 * @see IUploader
 * @see ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend
 */
public class Uploader extends BackendPart implements IUploader {
    private static final Logger log = LoggerFactory.getLogger(Uploader.class);

    // TODO make content type and min part size configurable
    private static final String CONTENT_TYPE = "application/binary";
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB

    public Uploader(final ConnectionConfig config) {
        super(config);
    }

    Uploader(final ConnectionConfig config, final MinioClient minioClient) {
        super(config, minioClient);
    }

    private void writeFileByPathToMinio(
            final Path srcPath,
            final String bucketName,
            final String objectName,
            final String entityName
    ) throws RemoteStorageException {

        final var localFilePath = srcPath.normalize().toAbsolutePath();

        try {
            // TODO may be change to ByteBufferedInputStream
            final var bytes = FileUtils.readFileToByteArray(localFilePath.toFile());
            final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            writeByteBufferToMinio(byteBuffer, bucketName, objectName, entityName);
        } catch (final IOException e) {
            log.error("Access to file {} IO error", localFilePath, e);
            throw new RemoteResourceNotFoundException(
                    entityName + " with path " + localFilePath + " doesn't exists or available", e);
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

    /**
     * Copies segment data from local filesystem to Minio S3
     *
     * @param srcPath path to source local segment file
     * @param dstObjectName S3 remote storage destination objectName
     * @throws RemoteStorageException on error
     */
    public void copySegmentData(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "segment data");
    }

    /**
     * Copies offset index from local filesystem to Minio
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @throws RemoteStorageException on error
     */
    public void copyOffsetIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "offset index");
    }

    /**
     * Copies time index from local filesystem to Minio
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @throws RemoteStorageException on error
     */
    public void copyTimeIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "time index");
    }

    /**
     * Copies transactional index from local filesystem to Minio
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @throws RemoteStorageException on error
     */
    public void copyTransactionalIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "transactional index");
    }

    /**
     * Copies producer snapshot index from local filesystem to Minio
     *
     * @param srcPath path to source local index file
     * @param dstObjectName S3 remote storage destination objectName
     * @throws RemoteStorageException on error
     */
    public void copyProducerSnapshotIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "producer snapshot index");
    }

    /**
     * Copies leader epoch index from local filesystem to Minio
     *
     * @param data index data byte buffer
     * @param dstObjectName S3 remote storage destination objectName
     * @throws RemoteStorageException on error
     */
    public void copyLeaderEpochIndex(final ByteBuffer data,  final String dstObjectName)
            throws RemoteStorageException {
        writeByteBufferToMinio(
                data,
                config.getMinioBucketName(),
                dstObjectName,
                "leader epoch index");
    }

}
