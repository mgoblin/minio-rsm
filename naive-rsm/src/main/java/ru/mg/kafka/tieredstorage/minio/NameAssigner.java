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

import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.apache.commons.lang3.Validate;

/**
 * Assign names to S3 objects based on log segment data file and indexes file names.
 *
 * <p>All Kafka partition segment files have same name and differs only by file extension.
 * File name contains first message offset
 *
 * <p>S3 object names are formatted as /[topicName]-[partition]/[file name].[file extension]<br/>
 * For example for partition 0 of topic named tieredTopic and segment data file name 0000975.log
 * S3 object assigned name will be /tieredTopic-0/0000975.log
 */
public class NameAssigner {
    private static final String SEGMENT_FILE_EXTENSION = "log";
    private static final String INDEX_FILE_EXTENSION = "index";
    private static final String TIME_INDEX_FILE_EXTENSION = "timeindex";
    private static final String TRANSACTIONAL_INDEX_FILE_EXTENSION = "txnindex";
    private static final String PRODUCER_SNAPSHOT_INDEX_FILE_EXTENSION = "snapshot";
    private static final String OBJECT_PATH_FORMAT = "/%s-%d/%020d";
    private static final String OBJECT_NAME_FORMAT = "%s.%s";

    private final String baseName;
    private final String topicName;
    private final int partition;

    /**
     * Create assigner using RemoteLogSegmentMetadata
     *
     * <p>Calculate and store base S3 object name</p>
     *
     * @param remoteLogSegmentMetadata {@literal @}NotNull metadata with topic name, partition number and start offset
     */
    public NameAssigner(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        Objects.requireNonNull(remoteLogSegmentMetadata);

        this.topicName = remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topic();
        this.partition = remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().partition();
        this.baseName = assignObjectName(remoteLogSegmentMetadata);

    }

    private String assignObjectName(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        final long startOffset = remoteLogSegmentMetadata.startOffset();

        try {
            Validate.notBlank(topicName, "topic name should be not blank");
            Validate.inclusiveBetween(0, Integer.MAX_VALUE, partition,
                    "partition should be greater or equals to zero");
            Validate.inclusiveBetween(0, Long.MAX_VALUE, startOffset,
                    "startOffset should be greater or equals to zero");
        } catch (final NullPointerException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

        return String.format(OBJECT_PATH_FORMAT, topicName, partition, startOffset);

    }

    /**
     * Get segment data S3 object name
     *
     * @return name as /[topic name]-[partition]/[base name].log
     */
    public String logSegmentObjectName() {
        return String.format(OBJECT_NAME_FORMAT, baseName, SEGMENT_FILE_EXTENSION);
    }

    /**
     * Get segment offset index S3 object name
     *
     * @return name as /[topic name]-[partition]/[base name].index
     */
    public String indexObjectName() {
        return String.format(OBJECT_NAME_FORMAT, baseName, INDEX_FILE_EXTENSION);
    }

    /**
     * Get segment time index S3 object name
     *
     * @return name as /[topic name]-[partition]/[base name].timeindex
     */
    public String timeIndexObjectName() {
        return String.format(OBJECT_NAME_FORMAT, baseName, TIME_INDEX_FILE_EXTENSION);
    }

    /**
     * Get segment transactional index S3 object name
     *
     * @return name as /[topic name]-[partition]/[base name].txnidex
     */
    public String transactionIndexObjectName() {
        return String.format(OBJECT_NAME_FORMAT, baseName, TRANSACTIONAL_INDEX_FILE_EXTENSION);
    }

    /**
     * Get segment producer snapshot S3 object name
     *
     * @return name as /[topic name]-[partition]/[base name].snapshot
     */
    public String producerSnapshotObjectName() {
        return String.format(OBJECT_NAME_FORMAT, baseName, PRODUCER_SNAPSHOT_INDEX_FILE_EXTENSION);
    }

    /**
     * Get segment leader epoch S3 object name
     *
     * @return name as /[topic name]-[partition]/leader-epoch-checkpoint
     */
    public String leaderEpochObjectName() {
        return String.format("/%s-%d/leader-epoch-checkpoint", topicName, partition);
    }

    /**
     * Get segment S3 object name by index type
     *
     * @param indexType {@literal @}NotNull index type. On null {@link IllegalArgumentException} throws
     * @return S3 object name for index type
     */
    public String indexNameByType(final RemoteStorageManager.IndexType indexType) {
        try {
            Objects.requireNonNull(indexType);
            return switch (indexType) {
                case OFFSET -> indexObjectName();
                case TIMESTAMP -> timeIndexObjectName();
                case PRODUCER_SNAPSHOT -> producerSnapshotObjectName();
                case TRANSACTION -> transactionIndexObjectName();
                case LEADER_EPOCH -> leaderEpochObjectName();
            };
        } catch (final NullPointerException e) {
            throw new IllegalArgumentException("Index type should be not null", e);
        }
    }

    /**
     * Get base name for S3 objects for Kafka local segment
     *
     * @return base name for S3 objects for Kafka local segment
     */
    public String getBaseName() {
        return baseName;
    }


    @Override
    public String toString() {
        return "PutS3Names{"
                + "logSegmentName='" + baseName + '\''
                + '}';
    }
}
