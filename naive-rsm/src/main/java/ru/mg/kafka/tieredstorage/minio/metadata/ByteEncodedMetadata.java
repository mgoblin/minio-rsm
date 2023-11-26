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

import org.apache.commons.lang3.Conversion;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

// TODO use only bitwise operators and eliminate using boolean[] on conversions
public final class ByteEncodedMetadata {
    private static final byte DATA_POSITION = 0;
    private static final byte OFFSET_INDEX_POSITION = 1;
    private static final byte TIME_INDEX_POSITION = 2;
    private static final byte TNX_INDEX_POSITION = 3;
    private static final  byte PRODUCER_SNAPSHOT_POSITION = 4;
    private static final byte LEADER_EPOCH_INDEX_POSITION = 5;
    private static final byte BITS_SIZE = 6;

    private byte value;

    public ByteEncodedMetadata(final byte value) {
        this.value = value;
        final boolean[] flags = asBooleanArray();
        this.value = toValue(flags);
    }

    public ByteEncodedMetadata() {
        super();
        this.value = 0;
    }

    private boolean[] asBooleanArray() {
        final boolean[] result = new boolean[BITS_SIZE];
        return Conversion.byteToBinary(value, 0, result, 0, BITS_SIZE);
    }

    private byte toValue(final boolean[] boolArray) {
        return Conversion.binaryToByte(boolArray, 0, (byte) 0, 0,  BITS_SIZE);
    }

    public boolean isDataNotEmpty() {
        return asBooleanArray()[DATA_POSITION];
    }

    public void setDataNotEmpty(final boolean dataNotEmpty) {
        final boolean[] current = asBooleanArray();
        current[DATA_POSITION] = dataNotEmpty;
        this.value = toValue(current);
    }

    public boolean isIndexNotEmpty() {
        return asBooleanArray()[OFFSET_INDEX_POSITION];
    }

    public void setIndexNotEmpty(final boolean indexNotEmpty) {
        final boolean[] current = asBooleanArray();
        current[OFFSET_INDEX_POSITION] = indexNotEmpty;
        this.value = toValue(current);
    }

    public boolean isTimeIndexNotEmpty() {
        return asBooleanArray()[TIME_INDEX_POSITION];
    }

    public void setTimeIndexNotEmpty(final boolean timeIndexNotEmpty) {
        final boolean[] current = asBooleanArray();
        current[TIME_INDEX_POSITION] = timeIndexNotEmpty;
        this.value = toValue(current);
    }

    public boolean isTransactionIndexNotEmpty() {
        return asBooleanArray()[TNX_INDEX_POSITION];
    }

    public void setTransactionIndexNotEmpty(final boolean transactionIndexNotEmpty) {
        final boolean[] current = asBooleanArray();
        current[TNX_INDEX_POSITION] = transactionIndexNotEmpty;
        this.value = toValue(current);
    }

    public boolean isProducerSnapshotIndexNotEmpty() {
        return asBooleanArray()[PRODUCER_SNAPSHOT_POSITION];
    }

    public void setProducerSnapshotIndexNotEmpty(final boolean producerSnapshotIndexNotEmpty) {
        final boolean[] current = asBooleanArray();
        current[PRODUCER_SNAPSHOT_POSITION] = producerSnapshotIndexNotEmpty;
        this.value = toValue(current);
    }

    public boolean isLeaderEpochIndexNotEmpty() {
        return asBooleanArray()[LEADER_EPOCH_INDEX_POSITION];
    }

    public void setLeaderEpochIndexNotEmpty(final boolean leaderEpochIndexNotEmpty) {
        final boolean[] current = asBooleanArray();
        current[LEADER_EPOCH_INDEX_POSITION] = leaderEpochIndexNotEmpty;
        this.value = toValue(current);
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
