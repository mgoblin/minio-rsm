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

import java.nio.ByteBuffer;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_ACCESS_KEY;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_AUTO_CREATE_BUCKET;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_BUCKET_NAME;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_S3_ENDPOINT_URL;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_SECRET_KEY;

@Testcontainers
public class DeferredInitRsmCopySegmentTest {

    public static final String MINIO_CONTAINER_NAME = "minio/minio:RELEASE.2023-09-04T19-57-37Z";
    public static final String MINIO_USER = "adminadmin";
    public static final String MINIO_PASSWORD = "adminadmin";

    public static final String BUCKET_NAME_VAL = "bucket";

    private DeferredInitRsm rsm;
    private MinIOContainer minIOContainer;
    private MinioClient minioClient;

    @BeforeEach
    public void setup() {
        rsm = new DeferredInitRsm();

        minIOContainer = new MinIOContainer(MINIO_CONTAINER_NAME)
                .withUserName(MINIO_USER)
                .withPassword(MINIO_PASSWORD);
        minIOContainer.start();

        final Map<String, ?> configs = Map.of(
                MINIO_S3_ENDPOINT_URL, minIOContainer.getS3URL(),
                MINIO_ACCESS_KEY, minIOContainer.getUserName(),
                MINIO_SECRET_KEY, minIOContainer.getPassword(),
                MINIO_BUCKET_NAME, BUCKET_NAME_VAL,
                MINIO_AUTO_CREATE_BUCKET, true
        );
        rsm.configure(configs);

        assertTrue(rsm.isInitialized());

        minioClient = MinioClient
                .builder()
                .endpoint(minIOContainer.getS3URL())
                .credentials(minIOContainer.getUserName(), minIOContainer.getPassword())
                .build();
    }

    @AfterEach
    public void tearDown() {
        rsm.close();
        minIOContainer.close();
    }

    private RemoteLogSegmentMetadata makeMetadata() {
        final TopicPartition topicPartition = new TopicPartition("topic1", 0);
        final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition);

        final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        return new RemoteLogSegmentMetadata(
                remoteLogSegmentId,
                0L, // start offset
                1L, // end offset
                0L, // maxTimestampMs
                0, // brokerId
                0L, // eventTimestampMs
                1, // segmentSizeInBytes
                Map.of(0, 0L) //segmentLeaderEpochs
        );
    }

    private LogSegmentData makeFakeLogSegmentData() {
        final Path logSegmentPath = Path.of("/tmp/nonexists");
        return new LogSegmentData(
                logSegmentPath,
                logSegmentPath,
                logSegmentPath,
                Optional.empty(),
                logSegmentPath,
                ByteBuffer.allocate(0));
    }

    private String readItemFromMinio(final String objectName) throws Exception {

        final GetObjectArgs getObjectArgs = GetObjectArgs.builder()
                .bucket(BUCKET_NAME_VAL)
                .object(objectName)
                .build();
        final var response = minioClient.getObject(getObjectArgs);
        return new String(response.readAllBytes());
    }

    private LogSegmentData makeLogSegmentDataWithoutTnx() {
        final String segmentDataPath = Objects.requireNonNull(this.getClass().getResource("/0.log")).getPath();
        final String segmentIndexPath = Objects.requireNonNull(this.getClass().getResource("/0.index")).getPath();
        final String segmentTimeIndexPath = Objects.requireNonNull(this.getClass().getResource("/0.time")).getPath();
        final String segmentSnapshotPath = Objects.requireNonNull(this.getClass().getResource("/0.snapshot")).getPath();
        return new LogSegmentData(
                Path.of(segmentDataPath),
                Path.of(segmentIndexPath),
                Path.of(segmentTimeIndexPath),
                Optional.empty(),
                Path.of(segmentSnapshotPath),
                ByteBuffer.wrap("leader epoch index".getBytes()));
    }

    private LogSegmentData makeLogSegmentDataWithTnx() {
        final String segmentDataPath = Objects.requireNonNull(this.getClass()
                .getResource("/0.log")).getPath();
        final String segmentIndexPath = Objects.requireNonNull(this.getClass()
                .getResource("/0.index")).getPath();
        final String segmentTimeIndexPath = Objects.requireNonNull(this.getClass()
                .getResource("/0.time")).getPath();
        final String segmentSnapshotPath = Objects.requireNonNull(this.getClass()
                .getResource("/0.snapshot")).getPath();
        final String segmentTnxIndexPath = Objects.requireNonNull(this.getClass()
                .getResource("/0.txn")).getPath();
        return new LogSegmentData(
                Path.of(segmentDataPath),
                Path.of(segmentIndexPath),
                Path.of(segmentTimeIndexPath),
                Optional.of(Path.of(segmentTnxIndexPath)),
                Path.of(segmentSnapshotPath),
                ByteBuffer.wrap("leader epoch index".getBytes()));
    }

    @Test
    public void testCopySegmentWithFakeData() {
        final RemoteStorageException ex = assertThrows(
                RemoteStorageException.class,
                () -> rsm.copyLogSegmentData(makeMetadata(), makeFakeLogSegmentData()));
        assertEquals("segment data with path /tmp/nonexists doesn't exists or available", ex.getMessage());
    }

    @Test
    public void testCopySegmentWithoutTxnIndex() throws Exception {
        final Optional<RemoteLogSegmentMetadata.CustomMetadata> optCustomMetadata = rsm.copyLogSegmentData(
                makeMetadata(),
                makeLogSegmentDataWithoutTnx());
        assertNotNull(optCustomMetadata);
        assertTrue(optCustomMetadata.isPresent());
        final RemoteLogSegmentMetadata.CustomMetadata customMetadata = optCustomMetadata.get();
        final byte[] value = customMetadata.value();
        assertEquals(1, value.length);
        final byte bitmask = value[0];
        assertEquals(55, bitmask);

        final var listArgs = ListObjectsArgs.builder()
                .bucket(BUCKET_NAME_VAL)
                .prefix("topic1-0")
                .recursive(true)
                .build();
        final Iterable<io.minio.Result<io.minio.messages.Item>> results = minioClient.listObjects(listArgs);
        final var resultsList = new ArrayList<io.minio.Result<io.minio.messages.Item>>();
        results.forEach(resultsList::add);
        final var items = resultsList.stream().map(x -> {
            try {
                return x.get().objectName();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();

        assertTrue(items.contains("topic1-0/00000000000000000000.log"));
        assertTrue(items.contains("topic1-0/00000000000000000000.index"));
        assertTrue(items.contains("topic1-0/00000000000000000000.timeindex"));
        assertTrue(items.contains("topic1-0/00000000000000000000.snapshot"));
        assertTrue(items.contains("topic1-0/00000000000000000000-leader-epoch-checkpoint"));
        assertEquals(5, items.size());

        final String logContent = readItemFromMinio(
                "topic1-0/00000000000000000000.log");
        assertEquals("segment log", logContent);

        final String indexContent = readItemFromMinio(
                "topic1-0/00000000000000000000.index");
        assertEquals("segment index", indexContent);

        final String timeIndexContent = readItemFromMinio(
                "topic1-0/00000000000000000000.timeindex");
        assertEquals("segment time index", timeIndexContent);

        final String snapshotContent = readItemFromMinio(
                "topic1-0/00000000000000000000.snapshot");
        assertEquals("segment snapshot", snapshotContent);

        final String leaderEpochContent = readItemFromMinio(
                "topic1-0/00000000000000000000-leader-epoch-checkpoint");
        assertEquals("leader epoch index", leaderEpochContent);
    }

    @Test
    public void testCopySegmentWithTxnIndex() throws Exception {
        final Optional<RemoteLogSegmentMetadata.CustomMetadata> optCustomMetadata = rsm.copyLogSegmentData(
                makeMetadata(),
                makeLogSegmentDataWithTnx());
        assertNotNull(optCustomMetadata);
        assertTrue(optCustomMetadata.isPresent());
        final RemoteLogSegmentMetadata.CustomMetadata customMetadata = optCustomMetadata.get();
        final byte[] value = customMetadata.value();
        assertEquals(1, value.length);
        final byte bitmask = value[0];
        assertEquals(63, bitmask);

        final var listArgs = ListObjectsArgs.builder()
                .bucket(BUCKET_NAME_VAL)
                .prefix("topic1-0")
                .recursive(true)
                .build();
        final Iterable<io.minio.Result<io.minio.messages.Item>> results = minioClient.listObjects(listArgs);
        final var resultsList = new ArrayList<io.minio.Result<io.minio.messages.Item>>();
        results.forEach(resultsList::add);
        final var items = resultsList.stream().map(x -> {
            try {
                return x.get().objectName();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();

        assertTrue(items.contains("topic1-0/00000000000000000000.log"));
        assertTrue(items.contains("topic1-0/00000000000000000000.index"));
        assertTrue(items.contains("topic1-0/00000000000000000000.timeindex"));
        assertTrue(items.contains("topic1-0/00000000000000000000.snapshot"));
        assertTrue(items.contains("topic1-0/00000000000000000000.txnindex"));
        assertTrue(items.contains("topic1-0/00000000000000000000-leader-epoch-checkpoint"));
        assertEquals(6, items.size());

        final String logContent = readItemFromMinio(
                "topic1-0/00000000000000000000.log");
        assertEquals("segment log", logContent);

        final String indexContent = readItemFromMinio(
                "topic1-0/00000000000000000000.index");
        assertEquals("segment index", indexContent);

        final String timeIndexContent = readItemFromMinio(
                "topic1-0/00000000000000000000.timeindex");
        assertEquals("segment time index", timeIndexContent);

        final String txnIndexContent = readItemFromMinio(
                "topic1-0/00000000000000000000.txnindex");
        assertEquals("segment transactional index", txnIndexContent);

        final String snapshotContent = readItemFromMinio(
                "topic1-0/00000000000000000000.snapshot");
        assertEquals("segment snapshot", snapshotContent);

        final String leaderEpochContent = readItemFromMinio(
                "topic1-0/00000000000000000000-leader-epoch-checkpoint");
        assertEquals("leader epoch index", leaderEpochContent);
    }
}
