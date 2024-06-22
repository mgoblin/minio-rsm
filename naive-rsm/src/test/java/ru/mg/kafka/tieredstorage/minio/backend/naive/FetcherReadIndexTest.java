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
import java.io.InputStream;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.ServerException;
import io.minio.messages.ErrorResponse;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FetcherReadIndexTest {

    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    @Test
    public void testReadIndex() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        when(minioClientMock.getObject(any())).thenReturn(new GetObjectResponse(
                Headers.of(), "bucket", "region", "object", InputStream.nullInputStream()
        ));

        final Fetcher fetcher = new Fetcher(config, minioClientMock);

        try (final InputStream stream = fetcher.readIndex("object", RemoteStorageManager.IndexType.OFFSET)) {
            assertNotNull(stream);
        }
    }

    @Test
    public void testReadIndexIOException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(IOException.class).when(minioClientMock).getObject(any());

        final Fetcher fetcher = new Fetcher(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () ->  {
                    try (final InputStream stream = fetcher.readIndex(
                        "object",
                        RemoteStorageManager.IndexType.OFFSET)) {
                        assertNotNull(stream);
                    }
                });
    }

    @Test
    public void testReadIndexServerException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(ServerException.class).when(minioClientMock).getObject(any());

        final Fetcher fetcher = new Fetcher(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> {
                    try (final InputStream stream = fetcher.readIndex(
                        "object",
                        RemoteStorageManager.IndexType.OFFSET)) {
                        assertNotNull(stream);
                    }
                });
    }

    @Test
    public void testReadIndexInsufficientDataException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(InsufficientDataException.class).when(minioClientMock).getObject(any());

        final Fetcher fetcher = new Fetcher(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> {
                    try (final InputStream stream = fetcher.readIndex(
                            "object",
                            RemoteStorageManager.IndexType.OFFSET)) {
                        assertNotNull(stream);
                    }
                });
    }

    @Test
    public void testReadIndexErrorResponseException() throws Exception {

        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        final var errorResponse = new ErrorResponse(
                "404",
                "Error",
                "bucket",
                "object",
                "resource",
                "RqUID",
                "host");
        final var rs = mock(Response.class);
        when(rs.request()).thenReturn(new Request(
                new HttpUrl(
                        "http",
                        "",
                        "",
                        "localhost",
                        80,
                        List.of(),
                        List.of(),
                        "",
                        ""),
                "http",
                Headers.of(),
                null,
                Map.of()
        ));

        doThrow(new ErrorResponseException(errorResponse, rs, "trace"))
                .when(minioClientMock).getObject(any());

        final Fetcher fetcher = new Fetcher(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> {
                    try (final InputStream stream = fetcher.readIndex(
                            "object",
                            RemoteStorageManager.IndexType.OFFSET)) {
                        assertNotNull(stream);
                    }
                });
    }

    @Test
    public void testReadIndexInternalException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(NoSuchAlgorithmException.class).when(minioClientMock).getObject(any());

        final Fetcher fetcher = new Fetcher(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> {
                    try (final InputStream stream = fetcher.readIndex(
                            "object",
                            RemoteStorageManager.IndexType.OFFSET)) {
                        assertNotNull(stream);
                    }
                });
    }

}
