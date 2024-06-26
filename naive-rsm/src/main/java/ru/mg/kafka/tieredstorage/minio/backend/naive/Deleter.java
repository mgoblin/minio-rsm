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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import ru.mg.kafka.tieredstorage.backend.IDeleter;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deleter implementation for Minio S3
 *
 * @see IDeleter
 * @see ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend
 */
public class Deleter extends BackendPart implements IDeleter {

    private static final Logger log = LoggerFactory.getLogger(Deleter.class);

    public Deleter(final ConnectionConfig config) {
        super(config);
    }

    public Deleter(final ConnectionConfig config, final MinioClient client) {
        super(config, client);
    }

    // TODO Warn if object doesn't exists
    public void deleteSegmentObject(final String objectName) throws RemoteStorageException {
        log.trace("Starting delete object {} from bucket {} and url {}",
                objectName,
                config.getMinioBucketName(),
                config.getMinioS3EndpointUrl());

        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(config.getMinioBucketName())
                            .object(objectName)
                            .build());
            log.debug("Object {} from bucket {} and url {} deleted",
                    objectName,
                    config.getMinioBucketName(),
                    config.getMinioS3EndpointUrl());

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
