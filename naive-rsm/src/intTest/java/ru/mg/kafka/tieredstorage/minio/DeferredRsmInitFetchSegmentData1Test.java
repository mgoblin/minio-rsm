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

import java.io.InputStream;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_ACCESS_KEY;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_AUTO_CREATE_BUCKET;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_BUCKET_NAME;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_S3_ENDPOINT_URL;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_SECRET_KEY;

@Testcontainers
public class DeferredRsmInitFetchSegmentData1Test {
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

    private RemoteLogSegmentMetadata makeMetadata(final byte[] customMetadataValue) {
        final RemoteLogSegmentMetadata.CustomMetadata customMetadata =
                new RemoteLogSegmentMetadata.CustomMetadata(customMetadataValue);
        final TopicPartition topicPartition = new TopicPartition("topic1", 0);
        final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition);

        final RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        return new RemoteLogSegmentMetadata(
                remoteLogSegmentId,
                0L, // start offset
                13L, // end offset
                0L, // maxTimestampMs
                0, // brokerId
                0L, // eventTimestampMs
                13, // segmentSizeInBytes
                Optional.of(customMetadata), // custom metadata
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                Map.of(0, 0L)
        );
    }

    private void putStringToMinio(final String content, final String objectName) throws Exception {
        try (final var is = IOUtils.toInputStream(content, "UTF-8")) {
            final var putArgs = PutObjectArgs.builder()
                    .bucket(BUCKET_NAME_VAL)
                    .object(objectName)
                    .stream(is, is.available(), -1)
                    .build();
            minioClient.putObject(putArgs);
        }
    }

    @Test
    public void testFetchSegmentData() throws Exception {
        final String segmentDataName = "/topic1-0/00000000000000000000.log";
        final String segmentData = "segment data";
        putStringToMinio(segmentData, segmentDataName);

        try (final InputStream is = rsm.fetchLogSegment(
                makeMetadata(new byte[] {1}),
                0)) {
            final byte[] content = is.readAllBytes();
            final String strContent = new String(content);
            assertEquals(segmentData, strContent);
        }
    }
}
