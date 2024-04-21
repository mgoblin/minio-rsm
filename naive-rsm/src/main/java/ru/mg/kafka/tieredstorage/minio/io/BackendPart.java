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

import java.util.Objects;

import io.minio.MinioClient;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

/**
 * Base abstract class for {@link ru.mg.kafka.tieredstorage.backend.IBucket}
 * {@link ru.mg.kafka.tieredstorage.backend.IDeleter}
 * {@link ru.mg.kafka.tieredstorage.backend.IFetcher}
 * {@link ru.mg.kafka.tieredstorage.backend.IUploader}
 * implementations.
 *
 * <p>Store connection config and create Minio S3 client.</p>
 */
public abstract class BackendPart {

    protected final ConnectionConfig config;
    protected final MinioClient minioClient;

    public BackendPart(final ConnectionConfig config) {
        Objects.requireNonNull(config, "Config should not be null");
        this.config = config;
        this.minioClient = MinioClient.builder()
                .endpoint(config.getMinioS3EndpointUrl())
                .credentials(config.getMinioAccessKey(), config.getMinioSecretKey().value())
                .build();
    }
}
