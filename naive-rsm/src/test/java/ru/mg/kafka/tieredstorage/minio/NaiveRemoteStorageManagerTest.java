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

import java.io.IOException;
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

import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;

import ru.mg.kafka.tieredstorage.minio.metadata.ByteEncodedMetadata;

import okhttp3.Headers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NaiveRemoteStorageManagerTest {
    @Test
    public void testCreateBucketIfNotExistsAndAutoCreateBucketIsTrue() throws Exception {

        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        when(minioClientMock.bucketExists(any()))
                .thenReturn(false);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key"
            ));

            assertTrue(remoteStorageManager.isInitialized());

            verify(minioClientMock, times(1)).bucketExists(any());
            verify(minioClientMock, times(1)).makeBucket(any());
        }

    }

    @Test
    public void testNotCreateBucketIfItExistsAndAutoCreateBucketIsTrue() throws Exception {

        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        when(minioClientMock.bucketExists(any()))
                .thenReturn(true);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key"
            ));

            assertTrue(remoteStorageManager.isInitialized());

            verify(minioClientMock, times(1)).bucketExists(any());
            verify(minioClientMock, times(0)).makeBucket(any());
        }

    }

    @Test
    public void testNotCreateBucketIfItExistsAndAutoCreateBucketIsFalse() throws Exception {

        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            assertTrue(remoteStorageManager.isInitialized());

            verify(minioClientMock, times(0)).bucketExists(any());
            verify(minioClientMock, times(0)).makeBucket(any());
        }

    }

    @Test
    public void testCreateBucketIfNotExistsAndAutoCreateBucketIsFalse() throws Exception {

        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            assertTrue(remoteStorageManager.isInitialized());

            verify(minioClientMock, times(0)).bucketExists(any());
            verify(minioClientMock, times(0)).makeBucket(any());
        }

    }

    @Test
    public void testNotInitializedAfterConstructor() {
        try (var manager = new NaiveRemoteStorageManager()) {
            assertFalse(manager.isInitialized());
            assertThrows(IllegalArgumentException.class, manager::getBucketName);
        }
    }

    @Test
    public void testMinioExceptionOnConfig() throws Exception {

        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        when(minioClientMock.bucketExists(any()))
                .thenAnswer(invocation -> {
                    throw new IOException();
                });

        try (final var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key"
            ));

            assertFalse(remoteStorageManager.isInitialized());
        }

        verify(minioClientMock, times(1)).bucketExists(any());
    }

    @Test
    public void testUninitializedAfterClose() {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            assertTrue(remoteStorageManager.isInitialized());

            remoteStorageManager.close();
            assertFalse(remoteStorageManager.isInitialized());
        }
    }

    @Test
    public void testCopyLogSegmentData() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            when(minioClientMock.putObject(any(io.minio.PutObjectArgs.class)))
                    .thenReturn(new io.minio.ObjectWriteResponse(
                            okhttp3.Headers.of(),
                            remoteStorageManager.getBucketName(),
                            "",
                            "",
                            "",
                            ""));


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

            verify(minioClientMock, times(6)).putObject(any(io.minio.PutObjectArgs.class));
        }

    }

    @Test
    public void testCopyLogSegmentDataWithoutTnxIndex() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            when(minioClientMock.putObject(any(io.minio.PutObjectArgs.class)))
                    .thenReturn(new io.minio.ObjectWriteResponse(
                            okhttp3.Headers.of(),
                            remoteStorageManager.getBucketName(),
                            "",
                            "",
                            "",
                            ""));


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

            verify(minioClientMock, times(5)).putObject(any(io.minio.PutObjectArgs.class));
        }

    }

    @Test
    public void testCopySegmentDataOnIOException() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            when(minioClientMock.putObject(any(io.minio.PutObjectArgs.class)))
                    .thenAnswer(invocation -> {
                        throw new IOException();
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

            verify(minioClientMock, times(1)).putObject(any(io.minio.PutObjectArgs.class));
        }
    }

    @Test
    public void testFetchSegmentFromStartPosition() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchLogSegment(
                    remoteLogSegmentMetadata,
                    0);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchLogSegmentFromStartPositionEmptyMetadata() {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            when(minioClientMock.getObject(any(GetObjectArgs.class)))
                    .thenAnswer(invocation -> {
                        throw new IOException();
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
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchLogSegment(
                    remoteLogSegmentMetadata,
                    0,
                    2000);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchLogSegmentFromStartToEndPositionEmptyMetadata() {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.OFFSET);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchTimeIndex() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.TIMESTAMP);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchTxnIndex() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.TRANSACTION);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }


    @Test
    public void testFetchProducerSnapshotIndex() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchLeaderEpochIndex() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            final GetObjectResponse response = new GetObjectResponse(
                    Headers.of(),
                    "bucket",
                    "region",
                    "object",
                    InputStream.nullInputStream());

            when(minioClientMock.getObject(any(io.minio.GetObjectArgs.class)))
                    .thenReturn(response);


            final var result = remoteStorageManager.fetchIndex(
                    remoteLogSegmentMetadata,
                    RemoteStorageManager.IndexType.LEADER_EPOCH);
            assertNotNull(result);

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchIndexWithoutMetadata() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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

            verify(minioClientMock, times(0)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchIndexCancelledByMetadata() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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

            verify(minioClientMock, times(0)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testFetchIndexOnIOException() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

            when(minioClientMock.getObject(any(GetObjectArgs.class)))
                    .thenAnswer(invocation -> {
                        throw new IOException();
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

            verify(minioClientMock, times(1)).getObject(any(GetObjectArgs.class));
        }
    }

    @Test
    public void testDeleteSegment() throws Exception {
        final var minioClientMock = mock(io.minio.MinioClient.class);
        Assertions.assertNotNull(minioClientMock);

        try (var remoteStorageManager = new NaiveRemoteStorageManager(minioClientMock)) {
            remoteStorageManager.configure(Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            ));

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


            doNothing().when(minioClientMock).removeObject(any());
            remoteStorageManager.deleteLogSegmentData(remoteLogSegmentMetadata);
            verify(minioClientMock, times(6)).removeObject(any());
        }
    }

}
