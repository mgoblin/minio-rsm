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

import java.io.InputStream;

import java.util.Map;

import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.errors.ServerException;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import okhttp3.Headers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleterTest {

    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    @Test
    public void testObjectExists() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        final var response = new GetObjectResponse(
                Headers.of(),
                "bucket",
                "region",
                "objectName",
                InputStream.nullInputStream()
        );
        when(minioClientMock.getObject(any())).thenReturn(response);

        final Deleter deleter = new Deleter(config, minioClientMock);

        assertTrue(deleter.objectExists("object"));

        verify(minioClientMock, times(1)).getObject(any());
    }

    @Test
    public void testObjectNotExists() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(ServerException.class).when(minioClientMock).getObject(any());

        final Deleter deleter = new Deleter(config, minioClientMock);

        assertFalse(deleter.objectExists("object"));
    }
}
