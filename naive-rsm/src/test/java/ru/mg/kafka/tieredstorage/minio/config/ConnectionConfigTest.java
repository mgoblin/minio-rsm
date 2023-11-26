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

package ru.mg.kafka.tieredstorage.minio.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ConnectionConfigTest {
    @Test
    public void testMinimalConfig() {
        final var url = "http://0.0.0.0:9000/";
        final var accessKey = "key";
        final var secretKey = "secret";
        final var defaultBucketName = "kafka-tiered-storage-bucket";

        final ConnectionConfig config = new ConnectionConfig(Map.of(
                "minio.url", url,
                "minio.access.key", accessKey,
                "minio.secret.key", secretKey
        ));

        assertEquals(url, config.getMinioS3EndpointUrl());
        assertEquals(accessKey, config.getMinioAccessKey());
        assertEquals(secretKey, config.getMinioSecretKey().value());
        assertEquals(defaultBucketName, config.getMinioBucketName());
        assertTrue(config.isAutoCreateBucket());
    }

    @Test
    public void testUrlNotNull() {
        final HashMap<String, String> config = new HashMap<>();
        config.put("minio.url", null);

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(config)
        );

        assertEquals(
                "Invalid value null for configuration minio.url: entry must be non null",
                ex.getMessage());
    }

    @Test
    public void testUrlNotBlank() {
        final var url = "";

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(Map.of("minio.url", url)));

        assertEquals(
                "Remote storage manager config is not valid: minio.url should not be blank",
                ex.getMessage());
    }

    @Test
    public void testInvalidUrlValidation() {
        final var url = "://";

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(Map.of("minio.url", url)));

        assertEquals(
                "Remote storage manager config is not valid: minio.url value :// is not valid URL",
                ex.getMessage());
    }

    @Test
    public void testAccessKeyNotNull() {
        final String url = "http://0.0.0.0:9000";

        final HashMap<String, String> config = new HashMap<>();
        config.put("minio.url", url);
        config.put("minio.access.key", null);

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(config)
        );

        assertEquals(
                "Invalid value null for configuration minio.access.key: entry must be non null",
                ex.getMessage());

    }

    @Test
    public void testAccessKeyNotBlank() {
        final var url = "http://127.0.0.1:9000";
        final var accessKey = "";

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(Map.of(
                        "minio.url", url,
                        "minio.access.key", accessKey)));

        assertEquals(
                "Remote storage manager config is not valid: minio.access.key should not be blank",
                ex.getMessage());
    }

    @Test
    public void testSecretKeyNotNull() {
        final String url = "http://0.0.0.0:9000";
        final String accessKey = "access";

        final HashMap<String, String> config = new HashMap<>();
        config.put("minio.url", url);
        config.put("minio.access.key", accessKey);
        config.put("minio.secret.key", null);

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(config)
        );

        assertEquals(
                "Invalid value null for configuration minio.secret.key: entry must be non null",
                ex.getMessage());

    }

    @Test
    public void testSecretKeyNotBlank() {
        final String url = "http://0.0.0.0:9000";
        final String accessKey = "access";
        final String secretKey = "";

        final HashMap<String, String> config = new HashMap<>();
        config.put("minio.url", url);
        config.put("minio.access.key", accessKey);
        config.put("minio.secret.key", secretKey);

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(config)
        );

        assertEquals(
                "Remote storage manager config is not valid: minio.secret.key should not be blank",
                ex.getMessage());
    }

    @Test
    public void testBucketNameNotNull() {
        final String url = "http://0.0.0.0:9000";
        final String accessKey = "access";
        final String secretKey = "key";

        final HashMap<String, String> config = new HashMap<>();
        config.put("minio.url", url);
        config.put("minio.access.key", accessKey);
        config.put("minio.secret.key", secretKey);
        config.put("minio.bucket.name", null);

        final var ex = assertThrows(
                ConfigException.class,
                () -> new ConnectionConfig(config)
        );

        assertEquals(
                "Remote storage manager config is not valid: minio.bucket.name should not be blank",
                ex.getMessage());
    }

    @Test
    public void testAutoCreateBucketExplicitTrue() {
        final var url = "http://0.0.0.0:9000/";
        final var accessKey = "key";
        final var secretKey = "secret";
        final var defaultBucketName = "kafka-tiered-storage-bucket";
        final var autoCreateBucket = true;

        final ConnectionConfig config = new ConnectionConfig(Map.of(
                "minio.url", url,
                "minio.access.key", accessKey,
                "minio.secret.key", secretKey,
                "minio.auto.create.bucket", autoCreateBucket
        ));

        assertEquals(url, config.getMinioS3EndpointUrl());
        assertEquals(accessKey, config.getMinioAccessKey());
        assertEquals(secretKey, config.getMinioSecretKey().value());
        assertEquals(defaultBucketName, config.getMinioBucketName());
        assertEquals(autoCreateBucket, config.isAutoCreateBucket());
    }

    @Test
    public void testAutoCreateBucketExplicitFalse() {
        final var url = "http://0.0.0.0:9000/";
        final var accessKey = "key";
        final var secretKey = "secret";
        final var defaultBucketName = "kafka-tiered-storage-bucket";
        final var autoCreateBucket = false;

        final ConnectionConfig config = new ConnectionConfig(Map.of(
                "minio.url", url,
                "minio.access.key", accessKey,
                "minio.secret.key", secretKey,
                "minio.auto.create.bucket", autoCreateBucket
        ));

        assertEquals(url, config.getMinioS3EndpointUrl());
        assertEquals(accessKey, config.getMinioAccessKey());
        assertEquals(secretKey, config.getMinioSecretKey().value());
        assertEquals(defaultBucketName, config.getMinioBucketName());
        assertEquals(autoCreateBucket, config.isAutoCreateBucket());
    }

}
