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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.mg.kafka.tieredstorage.minio.config.ConnectionConfig.MINIO_S3_ENDPOINT_URL;

public class DeferredInitRsmConfigIntegrationTest {

    private DeferredInitRsm rsm;

    @BeforeEach
    public void init() {
        rsm = new DeferredInitRsm();
    }

    @Test
    public void testIsInitializedAfterConstructor() {
        final boolean isInitialized = rsm.isInitialized();
        assertFalse(isInitialized);
    }

    @Test
    public void testConfigureWithEmptyConfig() {
        final ConfigException exception = assertThrows(
                ConfigException.class,
                () -> rsm.configure(Map.of()));
        final String message = exception.getMessage();
        assertEquals(
                "Remote storage manager config is not valid: minio.url should not be blank",
                message);

        assertEquals(Map.of(), rsm.getConfigs());
    }

    @Test
    public void testConfigureWithEmptyUrl() {
        final Map<String, String> config = Map.of(MINIO_S3_ENDPOINT_URL, "");

        final ConfigException exception = assertThrows(
                ConfigException.class,
                () -> rsm.configure(config));

        final String message = exception.getMessage();
        assertEquals(
                "Remote storage manager config is not valid: minio.url should not be blank",
                message);

        assertEquals(config, rsm.getConfigs());
    }

    @Test
    public void testConfigureBadFormedUrl() {
        final Map<String, String> config = Map.of(MINIO_S3_ENDPOINT_URL, "bad formed url");

        final ConfigException exception = assertThrows(
                ConfigException.class,
                () -> rsm.configure(config));

        final String message = exception.getMessage();
        assertEquals(
                "Remote storage manager config is not valid: minio.url value bad formed url is not valid URL",
                message);

        assertEquals(config, rsm.getConfigs());
    }

    @Test
    public void testConfigureNonHttpUtl() {
        final Map<String, String> config = Map.of(MINIO_S3_ENDPOINT_URL, "ftp://localhost");

        final ConfigException exception = assertThrows(
                ConfigException.class,
                () -> rsm.configure(config));

        final String message = exception.getMessage();
        assertEquals(
                "Remote storage manager config is not valid: minio.url value ftp://localhost is not valid URL",
                message);

        assertEquals(config, rsm.getConfigs());
    }

    @Test
    public void testConfigureOnlyValidUrl() {
        final Map<String, String> config = Map.of(MINIO_S3_ENDPOINT_URL, "http://127.0.0.1:9000");

        final ConfigException exception = assertThrows(
                ConfigException.class,
                () -> rsm.configure(config));

        final String message = exception.getMessage();
        assertEquals(
                "Remote storage manager config is not valid: minio.access.key should not be blank",
                message);

        assertEquals(config, rsm.getConfigs());
    }
}
