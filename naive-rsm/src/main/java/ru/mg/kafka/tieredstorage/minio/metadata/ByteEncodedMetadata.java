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

package ru.mg.kafka.tieredstorage.minio.metadata;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

// TODO Javadoc
public final class ByteEncodedMetadata {
    private static final byte DATA_POSITION = 0;
    private static final byte OFFSET_INDEX_POSITION = 1;
    private static final byte TIME_INDEX_POSITION = 2;
    private static final byte TNX_INDEX_POSITION = 3;
    private static final  byte PRODUCER_SNAPSHOT_POSITION = 4;
    private static final byte LEADER_EPOCH_INDEX_POSITION = 5;
    public static final int ALL_TRUE_VALUE = 0b00111111;
    public static final int ALL_FALSE_VALUE = 0;

    private byte value;

    public ByteEncodedMetadata(final byte value) {
        this.value = (byte) (value & ALL_TRUE_VALUE);
    }

    public ByteEncodedMetadata() {
        super();
        this.value = ALL_FALSE_VALUE;
    }

    public boolean isDataNotEmpty() {
        return (value & (1 << DATA_POSITION)) > 0;
    }

    public void setDataNotEmpty(final boolean dataNotEmpty) {
        final byte mask = 1 << DATA_POSITION;
        this.value = (byte) (dataNotEmpty ? this.value | mask : this.value & ~mask);
    }

    public boolean isIndexNotEmpty() {
        return (value & (1 << OFFSET_INDEX_POSITION)) > 0;
    }

    public void setIndexNotEmpty(final boolean indexNotEmpty) {
        final byte mask = 1 << OFFSET_INDEX_POSITION;
        this.value = (byte) (indexNotEmpty ? this.value | mask : this.value & ~mask);
    }

    public boolean isTimeIndexNotEmpty() {
        return (value & (1 << TIME_INDEX_POSITION)) > 0;
    }

    public void setTimeIndexNotEmpty(final boolean timeIndexNotEmpty) {
        final byte mask = 1 << TIME_INDEX_POSITION;
        this.value = (byte) (timeIndexNotEmpty ? this.value | mask : this.value & ~mask);
    }

    public boolean isTransactionIndexNotEmpty() {
        return (value & (1 << TNX_INDEX_POSITION)) > 0;
    }

    public void setTransactionIndexNotEmpty(final boolean transactionIndexNotEmpty) {
        final byte mask = 1 << TNX_INDEX_POSITION;
        this.value = (byte) (transactionIndexNotEmpty ? this.value | mask : this.value & ~mask);
    }

    public boolean isProducerSnapshotIndexNotEmpty() {
        return (value & (1 << PRODUCER_SNAPSHOT_POSITION)) > 0;
    }

    public void setProducerSnapshotIndexNotEmpty(final boolean producerSnapshotIndexNotEmpty) {
        final byte mask = 1 << PRODUCER_SNAPSHOT_POSITION;
        this.value = (byte) (producerSnapshotIndexNotEmpty ? this.value | mask : this.value & ~mask);
    }

    public boolean isLeaderEpochIndexNotEmpty() {
        return (value & (1 << LEADER_EPOCH_INDEX_POSITION)) > 0;
    }

    public void setLeaderEpochIndexNotEmpty(final boolean leaderEpochIndexNotEmpty) {
        final byte mask = 1 << LEADER_EPOCH_INDEX_POSITION;
        this.value = (byte) (leaderEpochIndexNotEmpty ? this.value | mask : this.value & ~mask);
    }

    public byte getValue() {
        return value;
    }

    public boolean isIndexOfTypePresent(final RemoteStorageManager.IndexType indexType) {
        return switch (indexType) {
            case OFFSET -> isIndexNotEmpty();
            case TIMESTAMP -> isTimeIndexNotEmpty();
            case PRODUCER_SNAPSHOT -> isProducerSnapshotIndexNotEmpty();
            case TRANSACTION -> isTransactionIndexNotEmpty();
            case LEADER_EPOCH -> isLeaderEpochIndexNotEmpty();
            case null, default -> throw new IllegalArgumentException("index type should be not null");
        };
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ByteEncodedMetadata that = (ByteEncodedMetadata) o;

        return new EqualsBuilder().append(value, that.value).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(value).toHashCode();
    }

    @Override
    public String toString() {
        return "ByteEncodedMetadata{"
                + "value=" + value
                + '}';
    }
}
