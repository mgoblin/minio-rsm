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

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import ru.mg.kafka.tieredstorage.backend.IBucket;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket manipulator implementation for Minio S3
 *
 * @see IBucket
 * @see ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend
 */
public class Bucket extends BackendPart implements IBucket {

    private static final Logger log = LoggerFactory.getLogger(Bucket.class);

    public Bucket(final ConnectionConfig config) {
        super(config);
    }

    Bucket(final ConnectionConfig config, final MinioClient clientMock) {
        super(config, clientMock);
    }

    /**
     * Create bucket if it does not exist
     *
     * @throws RecoverableConfigurationFailException on recoverable error
     */
    @Override
    public void tryToMakeBucket() throws RecoverableConfigurationFailException {
        try {
            if (config.isAutoCreateBucket()) {
                final boolean isBucketExists = minioClient.bucketExists(
                        BucketExistsArgs.builder().bucket(config.getMinioBucketName()).build());

                if (!isBucketExists) {
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket(config.getMinioBucketName()).build());
                    log.debug("Bucket {} created", config.getMinioBucketName());
                } else {
                    log.debug("Bucket {} already exists", config.getMinioBucketName());
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
