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

import java.io.IOException;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import ru.mg.kafka.tieredstorage.minio.backend.naive.RecoverableConfigurationFailException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class NaiveRsmConfigureTest {

    private static final Map<String, Object> MINIMAL_CFG = Map.of(
            "minio.url", "http://0.0.0.0",
            "minio.access.key", "access key",
            "minio.secret.key", "secret key"
            );

    private static final Map<String, Object> NOT_AUTO_CREATE_BUCKET_CFG = Map.of(
            "minio.url", "http://0.0.0.0",
            "minio.access.key", "access key",
            "minio.secret.key", "secret key",
            "minio.auto.create.bucket", false
    );

    @Test
    public void testTryToMakeBucket() throws RecoverableConfigurationFailException {
        final var backendMock = new MockedBackend(MINIMAL_CFG);
        final var bucketMock = backendMock.bucket();

        try (var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {

            assertTrue(remoteStorageManager.isInitialized());
            remoteStorageManager.configure(MINIMAL_CFG);

            verify(bucketMock, times(1)).tryToMakeBucket();
        }
    }

    @Test
    public void testRecoverableConfigurationFailExceptionOnConfig() throws RecoverableConfigurationFailException {
        final var backendMock = new MockedBackend(MINIMAL_CFG);

        doThrow(new RecoverableConfigurationFailException(new IOException()))
                .when(backendMock.bucket()).tryToMakeBucket();

        try (final var remoteStorageManager = new NaiveRemoteStorageManager(backendMock)) {
            assertTrue(remoteStorageManager.isInitialized());

            remoteStorageManager.configure(MINIMAL_CFG);
            assertFalse(remoteStorageManager.isInitialized());
        }
    }

    @Test
    public void testConfig() {

        try (final var remoteStorageManager = new NaiveRemoteStorageManager()) {
            assertFalse(remoteStorageManager.isInitialized());

            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CFG);
            assertTrue(remoteStorageManager.isInitialized());
        }
    }

    @Test
    public void testEnsureInitializedAfterConstructor() {
        try (final var remoteStorageManager = new NaiveRemoteStorageManager()) {
            assertFalse(remoteStorageManager.isInitialized());

            assertThrows(
                    RemoteStorageException.class,
                    remoteStorageManager::ensureInitialized);
        }
    }

    @Test
    public void testEnsureInitializedAfterConfigureSuccess() throws RemoteStorageException {
        try (final var remoteStorageManager = new NaiveRemoteStorageManager()) {
            assertFalse(remoteStorageManager.isInitialized());

            remoteStorageManager.configure(NOT_AUTO_CREATE_BUCKET_CFG);
            assertTrue(remoteStorageManager.isInitialized());

            remoteStorageManager.ensureInitialized();
            assertTrue(remoteStorageManager.isInitialized());
        }
    }

    @Test
    public void testEnsureInitializedAfterConfigureFail() throws RemoteStorageException {
        try (final var remoteStorageManager = new NaiveRemoteStorageManager()) {
            assertFalse(remoteStorageManager.isInitialized());

            assertThrows(ConfigException.class, () -> remoteStorageManager.configure(Map.of()));
            assertFalse(remoteStorageManager.isInitialized());

            assertThrows(ConfigException.class, remoteStorageManager::ensureInitialized);
            assertFalse(remoteStorageManager.isInitialized());

            remoteStorageManager.setConfigs(NOT_AUTO_CREATE_BUCKET_CFG);
            assertFalse(remoteStorageManager.isInitialized());

            remoteStorageManager.ensureInitialized();
            assertTrue(remoteStorageManager.isInitialized());
        }
    }
}
