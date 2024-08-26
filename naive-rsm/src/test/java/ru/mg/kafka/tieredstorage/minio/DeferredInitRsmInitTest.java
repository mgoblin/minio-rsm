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

import ru.mg.kafka.tieredstorage.minio.mock.MockedBackend;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class DeferredInitRsmInitTest {
    @Test
    public void testNotInitializedAfterConstructor() {
        try (var manager = new DeferredInitRsm()) {
            assertFalse(manager.isInitialized());
        }
    }

    @Test
    public void testUninitializedAfterClose() {

        final var cfg = Map.of(
                "minio.url", "http://0.0.0.0",
                "minio.access.key", "access key",
                "minio.secret.key", "secret key",
                "minio.auto.create.bucket", false
        );

        final var backendMock = new MockedBackend(cfg);

        try (var remoteStorageManager = new DeferredInitRsm()) {
            remoteStorageManager.setBackend(backendMock);
            remoteStorageManager.configure(cfg);

            assertTrue(remoteStorageManager.isInitialized());

            remoteStorageManager.close();
            assertFalse(remoteStorageManager.isInitialized());
        }
    }

}
