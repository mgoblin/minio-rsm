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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Fetcher {
    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    private final MinioClient minioClient;

    public ConnectionConfig getConfig() {
        return config;
    }

    private final ConnectionConfig config;

    public Fetcher(final Map<String, ?> configs) {
        this.config = new ConnectionConfig(configs);
        this.minioClient = MinioClient.builder()
                .endpoint(config.getMinioS3EndpointUrl())
                .credentials(config.getMinioAccessKey(), config.getMinioSecretKey().value())
                .build();
    }

    public byte[] fetchAllSegmentDataBytes(final String segmentObjectName) throws RemoteStorageException {
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
                    config.getMinioBucketName(),
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

    public InputStream fetchLogSegmentData(
            final String segmentObjectName,
            final int startPosition) throws RemoteStorageException {

        final byte[] body = fetchAllSegmentDataBytes(segmentObjectName);

        log.debug("Fetch log segment data from path {} success. "
                        + "Fetched {} bytes.",
                segmentObjectName,
                body.length);

        final byte[] subArray = Arrays.copyOfRange(body, startPosition, body.length);
        log.debug("Fetch log segment data from start position {} with path {} success. "
                        + "Fetched {} bytes, trimmed to {}.",
                startPosition,
                segmentObjectName,
                body.length,
                subArray.length);

        return new ByteArrayInputStream(subArray);
    }

    public InputStream fetchLogSegmentData(
            final String segmentObjectName,
            final int startPosition,
            final int endPosition) throws RemoteStorageException {

        final byte[] body = fetchAllSegmentDataBytes(segmentObjectName);

        log.debug("Fetch log segment data from path {} success. "
                        + "Fetched {} bytes.",
                segmentObjectName,
                body.length);

        final byte[] subArray = Arrays.copyOfRange(body, startPosition, endPosition);
        log.debug("Fetch log segment data from start position {} with path {} success. "
                        + "Fetched {} bytes, trimmed to {}.",
                startPosition,
                segmentObjectName,
                body.length,
                subArray.length);

        return new ByteArrayInputStream(subArray);
    }

    public InputStream readIndex(final String indexObjectName, final RemoteStorageManager.IndexType indexType)
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
                    config.getMinioBucketName(),
                    indexObjectName,
                    e.statusCode(),
                    e);
            throw new RemoteStorageException(e);
        } catch (final InsufficientDataException e) {
            log.error(
                    "Minio S3 bucket {} operation on putObject {} failed. "
                            + "Not enough data available in InputStream.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. "
                            + "Error response with code {} and message {}.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                       | XmlParserException | InternalException e) {
            log.error(
                    "Minio S3 bucket {} operation on getObject {} failed. Internal minio error.",
                    config.getMinioBucketName(),
                    indexObjectName,
                    e);
            throw new RemoteStorageException(e);
        }
    }

    public boolean objectExists(final String dataObjectName) {
        try (final var response = minioClient.getObject(GetObjectArgs.builder()
                .bucket(config.getMinioBucketName())
                .object(dataObjectName)
                .build())) {
            log.trace("Object {} found and have available {} bytes",
                    dataObjectName,
                    response.available());
            return true;
        } catch (final ServerException | InsufficientDataException | ErrorResponseException
                | IOException | NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                | XmlParserException | InternalException e) {
            log.error("Error getting object {}", dataObjectName, e);
            return false;
        }
    }

    public void deleteSegmentObject(final String objectName) throws RemoteStorageException {
        log.trace("Starting delete object {} from bucket {} and url {}",
                objectName,
                config.getMinioBucketName(),
                config.getMinioS3EndpointUrl());

        try {
            if (objectExists(objectName)) {
                log.trace("Object from deleting {} from bucket {} and url {} found",
                        objectName,
                        config.getMinioBucketName(),
                        config.getMinioS3EndpointUrl());

                minioClient.removeObject(
                        RemoveObjectArgs.builder()
                                .bucket(config.getMinioBucketName())
                                .object(objectName)
                                .build());
                log.debug("Object {} from bucket {} and url {} deleted",
                        objectName,
                        config.getMinioBucketName(),
                        config.getMinioS3EndpointUrl());
            } else {
                log.warn("Object {} for deletion from bucket {} and url {} does not exists",
                        objectName,
                        config.getMinioBucketName(),
                        config.getMinioS3EndpointUrl());
            }
        } catch (final IOException | ServerException e) {
            log.error("Delete {} from {} error. IO or server exception occurred.",
                    objectName,
                    config.getMinioBucketName(),
                    e);
            throw new RemoteStorageException(e);
        } catch (final ErrorResponseException e) {
            final var errorCode = e.errorResponse().code();
            final var errorMessage = e.errorResponse().message();
            log.error(
                    "Minio S3 bucket {} operation on removeObject {} failed. "
                            + "Error response with code {} and message {}.",
                    config.getMinioBucketName(),
                    objectName,
                    errorCode,
                    errorMessage,
                    e);
            throw new RemoteStorageException(e);
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException
                       | XmlParserException | InsufficientDataException | InternalException e) {
            log.error("Delete {} from {} error. Internal server exception occurred.",
                    objectName,
                    config.getMinioBucketName(),
                    e);
            throw new RemoteStorageException(e);
        }
    }
}
