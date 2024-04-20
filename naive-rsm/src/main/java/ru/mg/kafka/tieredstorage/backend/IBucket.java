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

import ru.mg.kafka.tieredstorage.minio.io.RecoverableConfigurationFailException;

/**
 * Bucket operations interface.
 * Declare S3 bucket operations.
 */
public interface IBucket {
    /**
     * Try to make S3 bucket if not exists
     *
     * @throws RecoverableConfigurationFailException on recoverable errors.
     *     If recoverable error occurred later will be another try.
     */
    void tryToMakeBucket() throws RecoverableConfigurationFailException;
}
