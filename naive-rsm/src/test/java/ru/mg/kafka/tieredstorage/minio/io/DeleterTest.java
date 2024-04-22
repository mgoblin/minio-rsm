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
import java.io.InputStream;

import java.util.List;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InternalException;
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    public void testDeleteExistingObject() throws Exception {
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

        deleter.deleteSegmentObject("object");

        verify(minioClientMock, times(1)).getObject(any());
        verify(minioClientMock, times(1)).removeObject(any());
    }

    @Test
    public void testDeleteNonExistingObject() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        doThrow(ServerException.class).when(minioClientMock).getObject(any());

        final Deleter deleter = new Deleter(config, minioClientMock);

        deleter.deleteSegmentObject("object");

        verify(minioClientMock, times(1)).getObject(any());
        verify(minioClientMock, times(0)).removeObject(any());
    }

    @Test
    public void testDeleteExistingObjectWithIOException() throws Exception {
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
        doThrow(IOException.class).when(minioClientMock).removeObject(any());

        final Deleter deleter = new Deleter(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> deleter.deleteSegmentObject("object"));

        verify(minioClientMock, times(1)).getObject(any());
        verify(minioClientMock, times(1)).removeObject(any());
    }

    @Test
    public void testDeleteExistingObjectWithErrorResponseException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);

        final var response = new GetObjectResponse(
                Headers.of(),
                "bucket",
                "region",
                "objectName",
                InputStream.nullInputStream()
        );

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
        when(minioClientMock.getObject(any())).thenReturn(response);
        doThrow(new ErrorResponseException(errorResponse, rs, "trace"))
                .when(minioClientMock).removeObject(any());

        final Deleter deleter = new Deleter(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> deleter.deleteSegmentObject("object"));

        verify(minioClientMock, times(1)).getObject(any());
        verify(minioClientMock, times(1)).removeObject(any());
    }

    @Test
    public void testDeleteExistingObjectWithInternalException() throws Exception {
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
        doThrow(InternalException.class).when(minioClientMock).removeObject(any());

        final Deleter deleter = new Deleter(config, minioClientMock);

        assertThrows(
                RemoteStorageException.class,
                () -> deleter.deleteSegmentObject("object"));

        verify(minioClientMock, times(1)).getObject(any());
        verify(minioClientMock, times(1)).removeObject(any());
    }
}
