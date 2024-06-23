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

package ru.mg.kafka.tieredstorage.minio.utils;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import java.util.Optional;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;

public interface LogSegmentDataUtils {
    static LogSegmentData logSegmentData() {
        final var logSegment = Path.of("./src/test/testData/test.log").normalize().toAbsolutePath();
        final var offsetIndex = Path.of("./src/test/testData/test.index");
        final var timeIndex = Path.of("./src/test/testData/test.timeindex");
        final Optional<Path> transactionalIndex = Optional.of(Path.of("./src/test/testData/test.txnindex"));
        final var producerSnapshotIndex = Path.of("./src/test/testData/test.snapshot");
        final var leaderEpochIndex = ByteBuffer.allocate(0);

        return new LogSegmentData(
                logSegment,
                offsetIndex,
                timeIndex,
                transactionalIndex,
                producerSnapshotIndex,
                leaderEpochIndex);
    }

    static LogSegmentData logSegmentDataWithoutTrnIndex() {
        final var logSegment = Path.of("./src/test/testData/test.log").normalize().toAbsolutePath();
        final var offsetIndex = Path.of("./src/test/testData/test.index");
        final var timeIndex = Path.of("./src/test/testData/test.timeindex");
        final Optional<Path> transactionalIndex = Optional.empty();
        final var producerSnapshotIndex = Path.of("./src/test/testData/test.snapshot");
        final var leaderEpochIndex = ByteBuffer.allocate(0);

        return new LogSegmentData(
                logSegment,
                offsetIndex,
                timeIndex,
                transactionalIndex,
                producerSnapshotIndex,
                leaderEpochIndex);
    }
}
