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

package ru.mg.kafka.tieredstorage.minio.utils;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

public interface MetadataUtils {

    private static TopicIdPartition topicIdPartitionMetadata(final String topicName, final int partition) {
        final Uuid topicUuid = Uuid.randomUuid();
        final TopicPartition topicPartition = new TopicPartition(topicName, partition);
        return new TopicIdPartition(topicUuid, topicPartition);
    }

    private static RemoteLogSegmentId segmentIdMetadata(final TopicIdPartition topicIdPartition) {
        final Uuid segmentUuid = Uuid.randomUuid();
        return new RemoteLogSegmentId(topicIdPartition, segmentUuid);
    }

    static RemoteLogSegmentMetadata remoteLogSegmentMetadata() {
        final String topicName = "tieredTopic";
        final int partition = 0;

        final TopicIdPartition topicIdPartition = topicIdPartitionMetadata(topicName, partition);
        final RemoteLogSegmentId remoteLogSegmentId = segmentIdMetadata(topicIdPartition);

        final long segmentStartOffset = 0L;
        final long segmentEndOffset = 1000L;
        final long segmentMaxTimestampMs = 10000L;
        final int brokerId = 0;
        final long segmentEventTimestampMs = 10001L;
        final int segmentSizeInBytes = 10;

        return new RemoteLogSegmentMetadata(
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
}
