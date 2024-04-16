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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig;
import ru.mg.kafka.tieredstorage.minio.io.Fetcher;
import ru.mg.kafka.tieredstorage.minio.io.Writer;
import ru.mg.kafka.tieredstorage.minio.metadata.ByteEncodedMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NaiveRemoteStorageManagerTest {

    @Test
    public void testCopyLogSegmentData() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            when(ioWriterMock.copySegmentData(any(), any())).thenReturn(true);
            when(ioWriterMock.copyOffsetIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyLeaderEpochIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyProducerSnapshotIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyTransactionalIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyTimeIndex(any(), any())).thenReturn(true);


            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));

            final var logSegment = Path.of("./src/test/testData/test.log").normalize().toAbsolutePath();
            final var offsetIndex = Path.of("./src/test/testData/test.index");
            final var timeIndex = Path.of("./src/test/testData/test.timeindex");
            final Optional<Path> transactionalIndex = Optional.of(Path.of("./src/test/testData/test.txnindex"));
            final var producerSnapshotIndex = Path.of("./src/test/testData/test.snapshot");
            final var leaderEpochIndex = ByteBuffer.allocate(0);

            final var logSegmentData = new LogSegmentData(
                    logSegment,
                    offsetIndex,
                    timeIndex,
                    transactionalIndex,
                    producerSnapshotIndex,
                    leaderEpochIndex);

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

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            when(ioWriterMock.copySegmentData(any(), any())).thenReturn(true);
            when(ioWriterMock.copyOffsetIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyLeaderEpochIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyProducerSnapshotIndex(any(), any())).thenReturn(true);
            when(ioWriterMock.copyTimeIndex(any(), any())).thenReturn(true);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));

            final var logSegment = Path.of("./src/test/testData/test.log").normalize().toAbsolutePath();
            final var offsetIndex = Path.of("./src/test/testData/test.index");
            final var timeIndex = Path.of("./src/test/testData/test.timeindex");
            final Optional<Path> transactionalIndex = Optional.empty();
            final var producerSnapshotIndex = Path.of("./src/test/testData/test.snapshot");
            final var leaderEpochIndex = ByteBuffer.allocate(0);

            final var logSegmentData = new LogSegmentData(
                    logSegment,
                    offsetIndex,
                    timeIndex,
                    transactionalIndex,
                    producerSnapshotIndex,
                    leaderEpochIndex);

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
            verify(ioWriterMock, times(0)).copyTransactionalIndex(any(), any());
            verify(ioWriterMock, times(1)).copyOffsetIndex(any(), any());
            verify(ioWriterMock, times(1)).copyProducerSnapshotIndex(any(), any());
            verify(ioWriterMock, times(1)).copyLeaderEpochIndex(any(), any());
        }

    }

    @Test
    public void testCopySegmentDataOnIOException() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            when(ioWriterMock.copySegmentData(any(), any()))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });


            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));

            final var logSegment = Path.of("./src/test/testData/test.log").normalize().toAbsolutePath();
            final var offsetIndex = Path.of("./src/test/testData/test.index");
            final var timeIndex = Path.of("./src/test/testData/test.timeindex");
            final Optional<Path> transactionalIndex = Optional.of(Path.of("./src/test/testData/test.txnindex"));
            final var producerSnapshotIndex = Path.of("./src/test/testData/test.snapshot");
            final var leaderEpochIndex = ByteBuffer.allocate(0);

            final var logSegmentData = new LogSegmentData(
                    logSegment,
                    offsetIndex,
                    timeIndex,
                    transactionalIndex,
                    producerSnapshotIndex,
                    leaderEpochIndex);

            assertThrows(
                    RemoteStorageException.class,
                    () -> remoteStorageManager.copyLogSegmentData(
                            remoteLogSegmentMetadata,
                            logSegmentData));

            verify(ioWriterMock, times(1)).copySegmentData(any(), any());
        }
    }

    @Test
    public void testFetchSegmentFromStartPosition() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.fetchLogSegmentData(any(), eq(0)))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchLogSegment(
                    remoteLogSegmentMetadata,
                    0);
            assertNotNull(result);

            final Fetcher verify = verify(ioFetcherMock, times(1));
            try (final var fetch = verify.fetchLogSegmentData(any(), eq(0))) {
                assertNull(fetch);
            }
        }
    }

    @Test
    public void testFetchLogSegmentFromStartPositionEmptyMetadata() {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 0));
        }
    }

    @Test
    public void testFetchLogSegmentFromStartPositionWithNoCopySegmentFlagMetadata() {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 62})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 0));
        }
    }

    @Test
    public void testFetchSegmentOnIOException() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            when(ioFetcherMock.fetchLogSegmentData(any(), any(Integer.class), any(Integer.class)))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });

            when(ioFetcherMock.fetchLogSegmentData(any(), eq(0)))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 0));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 0, 250));
        }
    }

    @Test
    public void testFetchSegmentFromStartAndEndPosition() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.fetchLogSegmentData(any(), eq(0), eq(2000)))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchLogSegment(
                    remoteLogSegmentMetadata,
                    0,
                    2000);
            assertNotNull(result);

            final Fetcher verify = verify(ioFetcherMock, times(1));
            try (final var fetch = verify.fetchLogSegmentData(any(), eq(0), eq(2000))) {
                assertNull(fetch);
            }

        }
    }

    @Test
    public void testFetchLogSegmentFromStartToEndPositionEmptyMetadata() {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 0, 200));
        }
    }

    @Test
    public void testFetchLogSegmentFromStartToEndPositionWithNoCopySegmentFlagMetadata() {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 62})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 0, 200));
        }
    }

    @Test
    public void testFetchOffsetIndex() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.readIndex(any(), any()))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.OFFSET);
            assertNotNull(result);

            final Fetcher verify = verify(ioFetcherMock, times(1));

            try (final var index = verify.readIndex(any(), any())) {
                assertNull(index);
            }
        }
    }

    @Test
    public void testFetchTimeIndex() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.readIndex(any(), any()))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.TIMESTAMP);
            assertNotNull(result);

            final Fetcher verify = verify(ioFetcherMock, times(1));
            try (final var index = verify.readIndex(any(), any())) {
                assertNull(index);
            }
        }
    }

    @Test
    public void testFetchTxnIndex() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.readIndex(any(), any()))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.TRANSACTION);
            assertNotNull(result);

            final Fetcher verify = verify(ioFetcherMock, times(1));
            try (final var index = verify.readIndex(any(), any())) {
                assertNull(index);
            }
        }
    }


    @Test
    public void testFetchProducerSnapshotIndex() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.readIndex(any(), any()))
                    .thenReturn(InputStream.nullInputStream());


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT);
            assertNotNull(result);

            final Fetcher verify = verify(ioFetcherMock, times(1));
            try (final var index = verify.readIndex(any(), any())) {
                assertNull(index);
            }
        }
    }

    @Test
    public void testFetchLeaderEpochIndex() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            when(ioFetcherMock.readIndex(any(), eq(RemoteStorageManager.IndexType.LEADER_EPOCH)))
                    .thenReturn(InputStream.nullInputStream());



            try (final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.LEADER_EPOCH)) {
                assertNotNull(result);
            }

            final Fetcher verify = verify(ioFetcherMock, times(1));
            try (final var index = verify.readIndex(any(), eq(RemoteStorageManager.IndexType.LEADER_EPOCH))) {
                assertNull(index);
            }
        }
    }

    @Test
    public void testFetchIndexWithoutMetadata() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchIndex(
                            remoteLogSegmentMetadata,
                            RemoteStorageManager.IndexType.LEADER_EPOCH));

            final Fetcher verify = verify(ioFetcherMock, times(0));
            try (final var index = verify.readIndex(any(), any())) {
                assertNull(index);
            }
        }
    }

    @Test
    public void testFetchIndexCancelledByMetadata() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 0})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));

            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchIndex(
                            remoteLogSegmentMetadata,
                            RemoteStorageManager.IndexType.LEADER_EPOCH));

            final Fetcher verify = verify(ioFetcherMock, times(0));
            try (final var index = verify.readIndex(any(), any())) {
                assertNull(index);
            }
        }
    }

    @Test
    public void testFetchIndexOnIOException() throws Exception {
        final var ioWriter = mock(Writer.class);
        final var ioFetcher = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcher.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriter, ioFetcher)) {
            remoteStorageManager.configure(cfg);

            when(ioFetcher.readIndex(any(), any()))
                    .thenAnswer(invocation -> {
                        throw new RemoteStorageException("");
                    });

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));


            assertThrows(RemoteStorageException.class,
                    () -> remoteStorageManager.fetchIndex(
                            remoteLogSegmentMetadata,
                            RemoteStorageManager.IndexType.OFFSET));
        }
    }

    @Test
    public void testDeleteSegment() throws Exception {
        final var ioWriterMock = mock(Writer.class);
        final var ioFetcherMock = mock(Fetcher.class);

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );
        when(ioFetcherMock.getConfig()).thenReturn(new ConnectionConfig(cfg));

        try (var remoteStorageManager = new NaiveRemoteStorageManager(ioWriterMock, ioFetcherMock)) {
            remoteStorageManager.configure(cfg);

            final String topicName = "tieredTopic";
            final int partition = 0;
            final TopicPartition topicPartition = new TopicPartition(topicName, partition);

            final Uuid topicUuid = Uuid.randomUuid();
            final TopicIdPartition topicIdPartition = new TopicIdPartition(topicUuid, topicPartition);

            final Uuid segmentUuid = Uuid.randomUuid();
            final long segmentStartOffset = 0L;
            final long segmentEndOffset = 1000L;
            final long segmentMaxTimestampMs = 10000L;
            final int brokerId = 0;
            final long segmentEventTimestampMs = 10001L;
            final int segmentSizeInBytes = 10;

            final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, segmentUuid);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
                    remoteLogSegmentId,
                    segmentStartOffset,
                    segmentEndOffset,
                    segmentMaxTimestampMs,
                    brokerId,
                    segmentEventTimestampMs,
                    segmentSizeInBytes,
                    Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(new byte[] {(byte) 63})),
                    RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                    Map.of(1, 0L));

            doNothing().when(ioFetcherMock).deleteSegmentObject(any());

            remoteStorageManager.deleteLogSegmentData(remoteLogSegmentMetadata);
            verify(ioFetcherMock, times(5)).deleteSegmentObject(any());
        }
    }

}
