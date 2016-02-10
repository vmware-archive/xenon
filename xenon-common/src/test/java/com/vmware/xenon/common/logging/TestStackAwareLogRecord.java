/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.xenon.common.logging;

import static org.junit.Assert.assertEquals;

import java.util.logging.Level;

import org.junit.Test;

public class TestStackAwareLogRecord {

    @Test
    public void withStackElement() {
        StackTraceElement stack = new StackTraceElement("class", "method", "file", -10);
        StackAwareLogRecord logRecord = new StackAwareLogRecord(Level.ALL, "msg");
        logRecord.setStackElement(stack);

        assertEquals("class", logRecord.getStackElementClassName());
        assertEquals("method", logRecord.getStackElementMethodName());
        assertEquals("file", logRecord.getStackElementFileName());
        assertEquals(-10, logRecord.getStackLineElementNumber());
    }

    @Test
    public void withoutStackElement() {
        StackAwareLogRecord logRecord = new StackAwareLogRecord(Level.ALL, "msg");

        // default values
        assertEquals(null, logRecord.getStackElementClassName());
        assertEquals(null, logRecord.getStackElementMethodName());
        assertEquals(null, logRecord.getStackElementFileName());
        assertEquals(-1, logRecord.getStackLineElementNumber());
    }
}
