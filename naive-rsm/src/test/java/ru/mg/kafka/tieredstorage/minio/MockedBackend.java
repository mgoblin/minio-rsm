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

package ru.mg.kafka.tieredstorage.minio;

import java.util.Map;

import ru.mg.kafka.tieredstorage.backend.IBucket;
import ru.mg.kafka.tieredstorage.backend.IDeleter;
import ru.mg.kafka.tieredstorage.backend.IFetcher;
import ru.mg.kafka.tieredstorage.backend.IUploader;
import ru.mg.kafka.tieredstorage.backend.RemoteStorageBackend;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Bucket;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Deleter;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Fetcher;
import ru.mg.kafka.tieredstorage.minio.backend.naive.Uploader;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import static org.mockito.Mockito.mock;

public class MockedBackend implements RemoteStorageBackend {

    private final ConnectionConfig config;
    private final Bucket bucket = mock(Bucket.class);
    private final Uploader uploader = mock(Uploader.class);
    private final Fetcher fetcher = mock(Fetcher.class);
    private final Deleter deleter = mock(Deleter.class);

    public MockedBackend(final Map<String, ?> configs) {
        this.config = new ConnectionConfig(configs);
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
