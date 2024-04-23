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

import java.nio.file.Path;

import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.MinioClient;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class UploaderCopyIndexesTest {
    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    @Test
    public void testCopyOffsetIndex() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        final Uploader uploader = new Uploader(config, minioClientMock);

        uploader.copyOffsetIndex(Path.of("./src/test/testData/test.index"), "testIndex");

    }

    @Test
    public void testCopyNonExistedOffsetIndex() {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        final Uploader uploader = new Uploader(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> uploader.copyOffsetIndex(Path.of(
                        "./src/test/testData/xxx.index"),
                        "testIndex"));
    }

    @Test
    public void testCopyOffsetIndexIOException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(IOException.class).when(minioClientMock).putObject(any());

        final Uploader uploader = new Uploader(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> uploader.copyOffsetIndex(Path.of(
                                "./src/test/testData/test.index"),
                        "testIndex"));
    }
}
