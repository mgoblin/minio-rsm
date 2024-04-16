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

package ru.mg.kafka.tieredstorage.minio.io;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer {
    private static final Logger log = LoggerFactory.getLogger(Writer.class);
    private static final String CONTENT_TYPE = "application/binary";
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB
    private final MinioClient minioClient;

    public ConnectionConfig getConfig() {
        return config != null ? config : new ConnectionConfig(Map.of());
    }

    private final ConnectionConfig config;

    public Writer(final Map<String, ?> configs) {
        this.config = new ConnectionConfig(configs);
        this.minioClient = MinioClient.builder()
                .endpoint(config.getMinioS3EndpointUrl())
                .credentials(config.getMinioAccessKey(), config.getMinioSecretKey().value())
                .build();
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

    public boolean copySegmentData(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "segment data");
        return true;
    }

    public boolean copyOffsetIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "offset index");
        return true;
    }

    public boolean copyTimeIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "time index");
        return true;
    }

    public boolean copyTransactionalIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "transactional index");
        return true;
    }

    public boolean copyProducerSnapshotIndex(final Path srcPath, final String dstObjectName)
            throws RemoteStorageException {
        writeFileByPathToMinio(
                srcPath,
                config.getMinioBucketName(),
                dstObjectName,
                "producer snapshot index");
        return true;
    }

    public boolean copyLeaderEpochIndex(final ByteBuffer data,  final String dstObjectName)
            throws RemoteStorageException {
        writeByteBufferToMinio(
                data,
                config.getMinioBucketName(),
                dstObjectName,
                "leader epoch index");
        return true;
    }

    public void tryToMakeBucket() throws RecoverableConfigurationFailException {

        try {
            if (config != null && config.isAutoCreateBucket()) {
                final boolean isBucketExists = minioClient.bucketExists(
                        BucketExistsArgs.builder().bucket(config.getMinioBucketName()).build());

                if (!isBucketExists) {
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket(config.getMinioBucketName()).build());
                    log.debug("Bucket {} created", config.getMinioBucketName());
                } else {
                    log.debug("Bucket {} found", config.getMinioBucketName());
                }
            }
        } catch (final IOException | ServerException | InternalException
                       | InsufficientDataException | ErrorResponseException e) {
            throw new RecoverableConfigurationFailException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                       | XmlParserException e) {
            log.error("Unrecoverable initialization error", e);
            throw new RuntimeException(e);
        }
    }

}
