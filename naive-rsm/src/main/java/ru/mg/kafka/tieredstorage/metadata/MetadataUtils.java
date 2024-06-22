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

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

public interface MetadataUtils {
    static ByteEncodedMetadata metadata(
            final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        final byte metadataBitmap = remoteLogSegmentMetadata.customMetadata()
                .orElse(new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{0})).value()[0];

        return new ByteEncodedMetadata(metadataBitmap);
    }

    static RemoteLogSegmentMetadata.CustomMetadata customMetadata(final byte value) {
        final byte[] metadataBitmap = new byte[]{value};
        return new RemoteLogSegmentMetadata.CustomMetadata(metadataBitmap);

    }
}
