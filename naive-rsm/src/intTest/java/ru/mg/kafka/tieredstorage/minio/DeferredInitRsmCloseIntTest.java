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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.fail;

public class DeferredInitRsmCloseIntTest {

    private DeferredInitRsm rsm;

    @BeforeEach
    public void setup() {
        rsm = new DeferredInitRsm();
    }

    @Test
    public void testCloseBeforeConfigure() {
        assertFalse(rsm.isInitialized());
        rsm.close();
        assertFalse(rsm.isInitialized());
    }

    @Test
    public void testCloseAfterConfigure() {
        //fail("Not implemented yet");
    }

    @Test
    public void testCloseTwice() {
        //fail("Not implemented yet");
    }
}
