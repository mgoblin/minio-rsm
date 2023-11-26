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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteEncodedMetadataTest {

    @Test
    void getValue() {
        final var byteEncodedMetadata = new ByteEncodedMetadata((byte) 0);
        assertEquals(0, byteEncodedMetadata.getValue());

        byteEncodedMetadata.setDataNotEmpty(true);
        assertEquals(1, byteEncodedMetadata.getValue());
        assertTrue(byteEncodedMetadata.isDataNotEmpty());

        byteEncodedMetadata.setIndexNotEmpty(true);
        assertEquals(3, byteEncodedMetadata.getValue());
        assertTrue(byteEncodedMetadata.isIndexNotEmpty());

        byteEncodedMetadata.setTimeIndexNotEmpty(true);
        assertEquals(7, byteEncodedMetadata.getValue());
        assertTrue(byteEncodedMetadata.isTimeIndexNotEmpty());

        byteEncodedMetadata.setTransactionIndexNotEmpty(true);
        assertEquals(15, byteEncodedMetadata.getValue());
        assertTrue(byteEncodedMetadata.isTransactionIndexNotEmpty());

        byteEncodedMetadata.setProducerSnapshotIndexNotEmpty(true);
        assertEquals(31, byteEncodedMetadata.getValue());
        assertTrue(byteEncodedMetadata.isProducerSnapshotIndexNotEmpty());

        byteEncodedMetadata.setLeaderEpochIndexNotEmpty(true);
        assertEquals(63, byteEncodedMetadata.getValue());
        assertTrue(byteEncodedMetadata.isLeaderEpochIndexNotEmpty());

        final var max = new  ByteEncodedMetadata(Byte.MAX_VALUE);
        assertEquals(63, max.getValue());
    }

    @Test
    void isIndexOfTypePresent() {
        final var byteEncodedMetadata = new  ByteEncodedMetadata((byte) 63);
        assertEquals(63, byteEncodedMetadata.getValue());

        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.OFFSET));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.TIMESTAMP));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.TRANSACTION));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT));
        assertTrue(byteEncodedMetadata.isIndexOfTypePresent(RemoteStorageManager.IndexType.LEADER_EPOCH));
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
        assertEquals(0, byteEncodedMetadata.getValue());
    }
}
