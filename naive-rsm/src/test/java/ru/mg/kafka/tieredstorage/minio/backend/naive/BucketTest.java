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

import java.security.NoSuchAlgorithmException;
import java.util.Map;

import io.minio.MinioClient;

import ru.mg.kafka.tieredstorage.minio.backend.RecoverableConfigurationFailException;
import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BucketTest {
    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    private static final Map<String, ?> AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", true
            );

    @Test
    public void testTryToMakeBucketOnAutoCreateFalse() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);
        final Bucket bucket = new Bucket(config, minioClientMock);

        bucket.tryToMakeBucket();

        verify(minioClientMock, times(0)).bucketExists(any());
    }

    @Test
    public void testMakeBucketOnAutoCreateTrue() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);
        final Bucket bucket = new Bucket(config, minioClientMock);

        when(minioClientMock.bucketExists(any())).thenReturn(false);

        bucket.tryToMakeBucket();

        verify(minioClientMock, times(1)).bucketExists(any());
        verify(minioClientMock, times(1)).makeBucket(any());
    }

    @Test
    public void testNotMakeBucketIfExists() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);
        final Bucket bucket = new Bucket(config, minioClientMock);

        when(minioClientMock.bucketExists(any())).thenReturn(true);

        bucket.tryToMakeBucket();

        verify(minioClientMock, times(1)).bucketExists(any());
        verify(minioClientMock, times(0)).makeBucket(any());
    }

    @Test
    public void testRecoverableException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);
        final Bucket bucket = new Bucket(config, minioClientMock);

        when(minioClientMock.bucketExists(any())).thenReturn(false);
        doThrow(IOException.class).when(minioClientMock).makeBucket(any());

        assertThrows(
                RecoverableConfigurationFailException.class,
                bucket::tryToMakeBucket);

        verify(minioClientMock, times(1)).bucketExists(any());
        verify(minioClientMock, times(1)).makeBucket(any());
    }

    @Test
    public void testRuntimeException() throws Exception {
        final ConnectionConfig config = new ConnectionConfig(AUTO_CREATE_BUCKET_CONFIG);
        final var minioClientMock = mock(MinioClient.class);
        final Bucket bucket = new Bucket(config, minioClientMock);

        when(minioClientMock.bucketExists(any())).thenReturn(false);
        doThrow(NoSuchAlgorithmException.class).when(minioClientMock).makeBucket(any());

        assertThrows(
                RuntimeException.class,
                bucket::tryToMakeBucket);

        verify(minioClientMock, times(1)).bucketExists(any());
        verify(minioClientMock, times(1)).makeBucket(any());
    }

}
