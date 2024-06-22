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

import java.io.InputStream;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import ru.mg.kafka.tieredstorage.backend.IFetcher;
import ru.mg.kafka.tieredstorage.metadata.ByteEncodedMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NaiveRsmFetchDataTest {

    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    @Test
    public void testFetchSegmentFromStartPosition() throws Exception {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
            byteEncodedMetadata.setDataNotEmpty(true);
            final RemoteLogSegmentMetadata.CustomMetadata customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(
                    new byte[] {byteEncodedMetadata.getByteValue()});

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.of(customMetadata),
                            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                            0
                    ));

            when(backendMock.fetcher().fetchLogSegmentData(any(), eq(0)))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchLogSegment(
                    remoteLogSegmentMetadata1,
                    0);
            assertNotNull(result);

            final IFetcher verify = verify(backendMock.fetcher(), times(1));
            try (final var fetch = verify.fetchLogSegmentData(any(), eq(0))) {
                assertNull(fetch);
            }
        }
    }

    @Test
    public void testFetchLogSegmentFromStartPositionEmptyMetadata() {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.empty(),
                            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                            0
                    ));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata1, 0));
        }
    }

    @Test
    public void testFetchLogSegmentFromStartPositionWithNoCopySegmentFlagMetadata() {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
            final RemoteLogSegmentMetadata.CustomMetadata customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(
                    new byte[] {byteEncodedMetadata.getByteValue()});

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.of(customMetadata),
                            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                            0
                    ));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata1, 0));
        }
    }

    @Test
    public void testFetchSegmentDataOnException() throws Exception {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

        final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
        byteEncodedMetadata.setDataNotEmpty(true);
        final RemoteLogSegmentMetadata.CustomMetadata customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(
                new byte[] {byteEncodedMetadata.getByteValue()});

        final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                new RemoteLogSegmentMetadataUpdate(
                        remoteLogSegmentMetadata.remoteLogSegmentId(),
                        0L,
                        Optional.of(customMetadata),
                        RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                        0
                ));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            when(backendMock.fetcher().fetchLogSegmentData(any(), any(Integer.class), any(Integer.class)))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });

            when(backendMock.fetcher().fetchLogSegmentData(any(), eq(0)))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata1, 0));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata1, 0, 250));
        }
    }

    @Test
    public void testFetchSegmentFromStartAndEndPosition() throws Exception {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
            byteEncodedMetadata.setDataNotEmpty(true);
            final RemoteLogSegmentMetadata.CustomMetadata customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(
                    new byte[] {byteEncodedMetadata.getByteValue()});

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.of(customMetadata),
                            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                            0
                    ));

            when(backendMock.fetcher().fetchLogSegmentData(any(), anyInt(), anyInt()))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchLogSegment(
                    remoteLogSegmentMetadata1,
                    0,
                    1000);
            assertNotNull(result);

            final IFetcher verify = verify(backendMock.fetcher(), times(1));
            try (final var fetch = verify.fetchLogSegmentData(any(), eq(0), eq(1000))) {
                assertNull(fetch);
            }
        }
    }

    @Test
    public void testFetchLogSegmentFromStartToEndPositionEmptyMetadata() {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.empty(),
                            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                            0
                    ));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata1, 0, 1000));
        }
    }

    @Test
    public void testFetchLogSegmentFromStartToEndPositionWithNoCopySegmentFlagMetadata() {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
            final RemoteLogSegmentMetadata.CustomMetadata customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(
                    new byte[] {byteEncodedMetadata.getByteValue()});

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.of(customMetadata),
                            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                            0
                    ));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata1, 0, 1000));
        }
    }

}
