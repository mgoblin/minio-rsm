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

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

/**
 * Remote storage backend is an abstraction over S3 implementations.
 *
 * <p>
 *     Backend consists from uploader, fetcher, deleter and bucket.
 *     Uploader abstracts copy segment data and indexes to S3.
 *     Fetcher abstracts segment data and index reading from S3.
 *     Deleter abstracts removing segment data and index from S3.
 *     Bucket abstracts operations over S3 bucket.
 * </p>
 */
public interface RemoteStorageBackend {
    /**
     * Get storage uploader
     * @return uploader
     */
    IUploader uploader();

    /**
     * Get storage fetcher
     * @return fetcher
     */
    IFetcher fetcher();

    /**
     * Get storage deleter
     * @return deleter
     */
    IDeleter deleter();

    /**
     * Get storage bucket manipulator
     * @return bucket manipulator
     */
    IBucket bucket();

    /**
     * Get storage connection configuration
     * @return connection configuration
     */
    ConnectionConfig getConfig();
}
