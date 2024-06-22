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

package ru.mg.kafka.tieredstorage.metadata;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteEncodedMetadataTest {

    @Test
    void testGetByteValue() {
        final var byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
        assertEquals(0, byteEncodedMetadata.getByteValue());

        byteEncodedMetadata.setDataNotEmpty(true);
        assertEquals(1, byteEncodedMetadata.getByteValue());
        assertTrue(byteEncodedMetadata.isDataNotEmpty());
        byteEncodedMetadata.setDataNotEmpty(false);
        assertEquals(0, byteEncodedMetadata.getByteValue());
        assertFalse(byteEncodedMetadata.isDataNotEmpty());
        byteEncodedMetadata.setDataNotEmpty(true);

        byteEncodedMetadata.setIndexNotEmpty(true);
        assertEquals(3, byteEncodedMetadata.getByteValue());
        assertTrue(byteEncodedMetadata.isIndexNotEmpty());
        byteEncodedMetadata.setIndexNotEmpty(false);
        assertEquals(1, byteEncodedMetadata.getByteValue());
        assertFalse(byteEncodedMetadata.isIndexNotEmpty());
        byteEncodedMetadata.setIndexNotEmpty(true);

        byteEncodedMetadata.setTimeIndexNotEmpty(true);
        assertEquals(7, byteEncodedMetadata.getByteValue());
        assertTrue(byteEncodedMetadata.isTimeIndexNotEmpty());
        byteEncodedMetadata.setTimeIndexNotEmpty(false);
        assertEquals(3, byteEncodedMetadata.getByteValue());
        assertFalse(byteEncodedMetadata.isTimeIndexNotEmpty());
        byteEncodedMetadata.setTimeIndexNotEmpty(true);

        byteEncodedMetadata.setTransactionIndexNotEmpty(true);
        assertEquals(15, byteEncodedMetadata.getByteValue());
        assertTrue(byteEncodedMetadata.isTransactionIndexNotEmpty());
        byteEncodedMetadata.setTransactionIndexNotEmpty(false);
        assertEquals(7, byteEncodedMetadata.getByteValue());
        assertFalse(byteEncodedMetadata.isTransactionIndexNotEmpty());
        byteEncodedMetadata.setTransactionIndexNotEmpty(true);

        byteEncodedMetadata.setProducerSnapshotIndexNotEmpty(true);
        assertEquals(31, byteEncodedMetadata.getByteValue());
        assertTrue(byteEncodedMetadata.isProducerSnapshotIndexNotEmpty());
        byteEncodedMetadata.setProducerSnapshotIndexNotEmpty(false);
        assertEquals(15, byteEncodedMetadata.getByteValue());
        assertFalse(byteEncodedMetadata.isProducerSnapshotIndexNotEmpty());
        byteEncodedMetadata.setProducerSnapshotIndexNotEmpty(true);

        byteEncodedMetadata.setLeaderEpochIndexNotEmpty(true);
        assertEquals(63, byteEncodedMetadata.getByteValue());
        assertTrue(byteEncodedMetadata.isLeaderEpochIndexNotEmpty());
        byteEncodedMetadata.setLeaderEpochIndexNotEmpty(false);
        assertEquals(31, byteEncodedMetadata.getByteValue());
        assertFalse(byteEncodedMetadata.isLeaderEpochIndexNotEmpty());
        byteEncodedMetadata.setLeaderEpochIndexNotEmpty(true);

        final var max = new  ByteEncodedMetadata((byte) 0b11111111);
        assertEquals(63, max.getByteValue());
    }

    @Test
    void isIndexOfTypePresent() {
        final var byteEncodedMetadata = new ByteEncodedMetadata((byte) 63);
        assertEquals(63, byteEncodedMetadata.getByteValue());

        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.OFFSET));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.TIMESTAMP));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.TRANSACTION));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.LEADER_EPOCH));

        assertThrows(IllegalArgumentException.class,
                () -> byteEncodedMetadata.isIndexOfTypePresent(null));
    }

    @Test
    public void testEqualsAndHashCode() {
        final var byteEncodedMetadata1 = new ByteEncodedMetadata((byte) 5);
        final var byteEncodedMetadata2 = new ByteEncodedMetadata((byte) 5);
        final var byteEncodedMetadata3 = new ByteEncodedMetadata((byte) 1);

        assertEquals(byteEncodedMetadata1, byteEncodedMetadata1);
        assertFalse(byteEncodedMetadata1.equals(null));
        assertFalse(byteEncodedMetadata1.equals(new Object()));

        assertEquals(byteEncodedMetadata1, byteEncodedMetadata2);
        assertEquals(byteEncodedMetadata1.hashCode(), byteEncodedMetadata2.hashCode());


        assertNotEquals(byteEncodedMetadata1, byteEncodedMetadata3);
        assertNotEquals(byteEncodedMetadata1.hashCode(), byteEncodedMetadata3.hashCode());
    }

    @Test
    public void testToString() {
        final var byteEncodedMetadata = new ByteEncodedMetadata((byte) 3);
        assertEquals("ByteEncodedMetadata{value=3}", byteEncodedMetadata.toString());
    }

    @Test
    public void testDefaultConstructor() {
        final var byteEncodedMetadata = new ByteEncodedMetadata();
        assertEquals(0, byteEncodedMetadata.getByteValue());
    }
}
