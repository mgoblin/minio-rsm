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

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import ru.mg.kafka.tieredstorage.metadata.ByteEncodedMetadata;
import ru.mg.kafka.tieredstorage.minio.mock.MockedBackend;
import ru.mg.kafka.tieredstorage.minio.utils.MetadataUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DeferredInitRsmDeleteTest {

    private static final Map<String, ?> NOT_AUTO_CREATE_BUCKET_CONFIG =
            Map.of(
                    "minio.url", "http://0.0.0.0",
                    "minio.access.key", "access key",
                    "minio.secret.key", "secret key",
                    "minio.auto.create.bucket", false
            );

    @Test
    public void testDeleteSegment() throws Exception {
        final var backendMock = new MockedBackend(NOT_AUTO_CREATE_BUCKET_CONFIG);

        try (var remoteStorageManager = new DeferredInitRsm()) {
            remoteStorageManager.setBackend(backendMock);
            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CONFIG);

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata = MetadataUtils.remoteLogSegmentMetadata();

            final ByteEncodedMetadata byteEncodedMetadata = new ByteEncodedMetadata((byte) 63);

            final RemoteLogSegmentMetadata.CustomMetadata customMetadata = new RemoteLogSegmentMetadata.CustomMetadata(
                    new byte[] {byteEncodedMetadata.getByteValue()});

            final RemoteLogSegmentMetadata remoteLogSegmentMetadata1 = remoteLogSegmentMetadata.createWithUpdates(
                    new RemoteLogSegmentMetadataUpdate(
                            remoteLogSegmentMetadata.remoteLogSegmentId(),
                            0L,
                            Optional.of(customMetadata),
                            RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                            0
                    ));


            doNothing().when(backendMock.deleter()).deleteSegmentObject(any());

            remoteStorageManager.deleteLogSegmentData(remoteLogSegmentMetadata1);
            verify(backendMock.deleter(), times(6)).deleteSegmentObject(any());
        }
    }
}
