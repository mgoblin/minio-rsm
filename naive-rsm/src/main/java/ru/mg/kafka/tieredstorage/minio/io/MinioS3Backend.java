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

import java.util.Map;

import ru.mg.kafka.tieredstorage.backend.IBucket;
import ru.mg.kafka.tieredstorage.backend.IDeleter;
import ru.mg.kafka.tieredstorage.backend.IFetcher;
import ru.mg.kafka.tieredstorage.backend.IUploader;
import ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

public class MinioS3Backend implements RemoteStorageBackend {

    private final ConnectionConfig config;
    private final Deleter deleter;
    private final Fetcher fetcher;
    private final Uploader uploader;
    private final Bucket bucket;

    public MinioS3Backend(final Map<String, ?> configs) {
        this.config = new ConnectionConfig(configs);
        this.deleter = new Deleter(config);
        this.fetcher = new Fetcher(config);
        this.uploader = new Uploader(config);
        this.bucket = new Bucket(config);
    }

    @Override
    public IUploader uploader() {
        return uploader;
    }

    @Override
    public IFetcher fetcher() {
        return fetcher;
    }

    @Override
    public IDeleter deleter() {
        return deleter;
    }

    @Override
    public IBucket bucket() {
        return bucket;
    }

    @Override
    public ConnectionConfig getConfig() {
        return config;
    }
}
