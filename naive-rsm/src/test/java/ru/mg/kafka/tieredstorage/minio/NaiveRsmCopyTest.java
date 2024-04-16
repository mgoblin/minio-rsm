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

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;
import ru.mg.kafka.tieredstorage.minio.io.Fetcher;
import ru.mg.kafka.tieredstorage.minio.io.Writer;
import ru.mg.kafka.tieredstorage.minio.metadata.ByteEncodedMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NaiveRsmCopyTest {

    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    @Test
    public void testCopyLogSegmentData() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            when(ioWriterMock.copySegmentData(any(), any())).thenReturn(true);
            when(ioWriterMock.copyOffsetIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyLeaderEpochIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyProducerSnapshotIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyTransactionalIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyTimeIndex(any(), any())).thenReturn(true);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();
            final var logSegmentData = LogSegmentDataUtils.logSegmentData();

            final var customMetadataActual = remoteStorageManager.copyLogSegmentData(
                    remoteLogSegmentMetadata,
                    logSegmentData);

            final var copyMetadataExpected = new ByteEncodedMetadata();
            copyMetadataExpected.setDataNotEmpty(true);
            copyMetadataExpected.setIndexNotEmpty(true);
            copyMetadataExpected.setTimeIndexNotEmpty(true);
            copyMetadataExpected.setTransactionIndexNotEmpty(true);
            copyMetadataExpected.setProducerSnapshotIndexNotEmpty(true);
            copyMetadataExpected.setLeaderEpochIndexNotEmpty(true);

            assertTrue(customMetadataActual.isPresent());
            final var copyMetadataActual = new ByteEncodedMetadata(customMetadataActual.get().value()[0]);

            assertEquals(copyMetadataExpected, copyMetadataActual);

            verify(ioWriterMock, times(1)).copySegmentData(any(), any());
            verify(ioWriterMock, times(1)).copyTimeIndex(any(), any());
            verify(ioWriterMock, times(1)).copyTransactionalIndex(any(), any());
            verify(ioWriterMock, times(1)).copyOffsetIndex(any(), any());
            verify(ioWriterMock, times(1)).copyProducerSnapshotIndex(any(), any());
            verify(ioWriterMock, times(1)).copyLeaderEpochIndex(any(), any());
        }
    }

    @Test
    public void testCopyLogSegmentDataWithoutTnxIndex() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            when(ioWriterMock.copySegmentData(any(), any())).thenReturn(true);
            when(ioWriterMock.copyOffsetIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyLeaderEpochIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyProducerSnapshotIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyTimeIndex(any(), any())).thenReturn(true);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();
            final var logSegmentData = LogSegmentDataUtils.logSegmentData();

            final var customMetadataActual = remoteStorageManager.copyLogSegmentData(
                    remoteLogSegmentMetadata,
                    logSegmentData);

            final var copyMetadataExpected = new ByteEncodedMetadata();
            copyMetadataExpected.setDataNotEmpty(true);
            copyMetadataExpected.setIndexNotEmpty(true);
            copyMetadataExpected.setTimeIndexNotEmpty(true);
            copyMetadataExpected.setTransactionIndexNotEmpty(false);
            copyMetadataExpected.setProducerSnapshotIndexNotEmpty(true);
            copyMetadataExpected.setLeaderEpochIndexNotEmpty(true);

            assertTrue(customMetadataActual.isPresent());
            final var copyMetadataActual = new ByteEncodedMetadata(customMetadataActual.get().value()[0]);

            assertEquals(copyMetadataExpected.getByteValue(), copyMetadataActual.getByteValue());

            verify(ioWriterMock, times(1)).copySegmentData(any(), any());
            verify(ioWriterMock, times(1)).copyTimeIndex(any(), any());
            verify(ioWriterMock, times(1)).copyOffsetIndex(any(), any());
            verify(ioWriterMock, times(1)).copyProducerSnapshotIndex(any(), any());
            verify(ioWriterMock, times(1)).copyLeaderEpochIndex(any(), any());
        }

    }

    @Test
    public void testCopySegmentDataOnIOException() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(NOT_AUTO_CREATE_BUCKET_CONFIG));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            when(ioWriterMock.copySegmentData(any(), any()))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();
            final var logSegmentData = LogSegmentDataUtils.logSegmentData();

            assertThrows(
                    RemoteStorageException.class,
                    () -> remoteStorageManager.copyLogSegmentData(
                            remoteLogSegmentMetadata,
                            logSegmentData));

            verify(ioWriterMock, times(1)).copySegmentData(any(), any());
        }
    }

}
