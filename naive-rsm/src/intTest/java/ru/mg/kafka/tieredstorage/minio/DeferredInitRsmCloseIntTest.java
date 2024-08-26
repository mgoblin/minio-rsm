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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_ACCESS_KEY;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_AUTO_CREATE_BUCKET;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_BUCKET_NAME;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_S3_ENDPOINT_URL;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_SECRET_KEY;

@Testcontainers
public class DeferredInitRsmCloseIntTest {

    public static final String MINIO_CONTAINER_NAME = "minio/minio:RELEASE.2023-09-04T19-57-37Z";
    public static final String MINIO_USER = "adminadmin";
    public static final String MINIO_PASSWORD = "adminadmin";

    public static final String BUCKET_NAME_VAL = "bucket";

    private DeferredInitRsm rsm;
    private MinIOContainer minIOContainer;

    @BeforeEach
    public void setup() {
        rsm = new DeferredInitRsm();

        minIOContainer = new MinIOContainer(MINIO_CONTAINER_NAME)
                .withUserName(MINIO_USER)
                .withPassword(MINIO_PASSWORD);
        minIOContainer.start();
    }

    @AfterEach
    public void tearDown() {
        minIOContainer.close();
    }

    @Test
    public void testCloseBeforeConfigure() {
        assertFalse(rsm.isInitialized());
        rsm.close();
        assertFalse(rsm.isInitialized());
    }

    @Test
    public void testCloseAfterConfigure() {
        assertFalse(rsm.isInitialized());

        final Map<String, ?> configs = Map.of(
                MINIO_S3_ENDPOINT_URL, minIOContainer.getS3URL(),
                MINIO_ACCESS_KEY, minIOContainer.getUserName(),
                MINIO_SECRET_KEY, minIOContainer.getPassword(),
                MINIO_BUCKET_NAME, BUCKET_NAME_VAL,
                MINIO_AUTO_CREATE_BUCKET, true
        );
        rsm.configure(configs);

        assertTrue(rsm.isInitialized());

        rsm.close();
        assertFalse(rsm.isInitialized());
    }

    @Test
    public void testCloseTwice() {
        assertFalse(rsm.isInitialized());
        rsm.close();
        assertFalse(rsm.isInitialized());
        rsm.close();
        assertFalse(rsm.isInitialized());

        final Map<String, ?> configs = Map.of(
                MINIO_S3_ENDPOINT_URL, minIOContainer.getS3URL(),
                MINIO_ACCESS_KEY, minIOContainer.getUserName(),
                MINIO_SECRET_KEY, minIOContainer.getPassword(),
                MINIO_BUCKET_NAME, BUCKET_NAME_VAL,
                MINIO_AUTO_CREATE_BUCKET, true
        );
        rsm.configure(configs);

        assertTrue(rsm.isInitialized());

        rsm.close();
        assertFalse(rsm.isInitialized());
    }
}
