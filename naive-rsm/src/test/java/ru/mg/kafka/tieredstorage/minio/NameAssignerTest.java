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
import java.util.Optional;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NameAssignerTest {

    public static RemoteLogSegmentMetadata remoteLogSegmentMetadata;

    @BeforeAll
    public static void beforeAll() {
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

        remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
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

    }

    @Test
    void testLogSegmentObjectNameAssign() {
        final NameAssigner names = new NameAssigner(remoteLogSegmentMetadata);

        assertEquals("/tieredTopic-0/00000000000000000000.log", names.logSegmentObjectName());
    }

    @Test void testIndexObjectNameAssign() {
        final NameAssigner names = new NameAssigner(remoteLogSegmentMetadata);

        assertEquals("/tieredTopic-0/00000000000000000000.index", names.indexObjectName());
    }

    @Test void testTimeIndexObjectNameAssign() {
        final NameAssigner names = new NameAssigner(remoteLogSegmentMetadata);

        assertEquals("/tieredTopic-0/00000000000000000000.timeindex", names.timeIndexObjectName());
    }

    @Test
    public void testIndexNameByType() {
        final NameAssigner names = new NameAssigner(remoteLogSegmentMetadata);

        assertEquals(
                "/tieredTopic-0/00000000000000000000.index",
                names.indexNameByType(RemoteStorageManager.IndexType.OFFSET));
        assertEquals(
                "/tieredTopic-0/00000000000000000000.timeindex",
                names.indexNameByType(RemoteStorageManager.IndexType.TIMESTAMP));

        assertEquals(
                "/tieredTopic-0/00000000000000000000.txnindex",
                names.indexNameByType(RemoteStorageManager.IndexType.TRANSACTION));

        assertEquals(
                "/tieredTopic-0/00000000000000000000.snapshot",
                names.indexNameByType(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT));

        assertEquals(
                "/tieredTopic-0/00000000000000000000-leader-epoch-checkpoint",
                names.indexNameByType(RemoteStorageManager.IndexType.LEADER_EPOCH));

        assertThrows(IllegalArgumentException.class, () -> names.indexNameByType(null));
    }

    @Test
    public void testBaseName() {
        final NameAssigner names = new NameAssigner(remoteLogSegmentMetadata);
        assertEquals("/tieredTopic-0/00000000000000000000", names.getBaseName());
    }

    @Test
    public void testToString() {
        final NameAssigner names = new NameAssigner(remoteLogSegmentMetadata);
        assertEquals("PutS3Names{logSegmentName='/tieredTopic-0/00000000000000000000'}", names.toString());
    }

    @Test
    public void NullTopicName() {
        final String topicName = null;
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

        final var metadata = new RemoteLogSegmentMetadata(
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

        assertThrows(IllegalArgumentException.class, () -> new NameAssigner(metadata));
    }

    @Test
    public void testNegativePartition() {
        final String topicName = "topic1";
        final int partition = -1;
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

        final var metadata = new RemoteLogSegmentMetadata(
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

        assertThrows(IllegalArgumentException.class, () -> new NameAssigner(metadata));

    }

}
