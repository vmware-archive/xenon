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

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * LogRecord that contains invoked line of code StackTraceElement.
 *
 * StackTraceElement contains detailed information about where log method was called.
 */
public class StackAwareLogRecord extends LogRecord {

    private static final long serialVersionUID = 2233534645443849237L;
    private StackTraceElement stackElement;

    public StackAwareLogRecord(Level level, String msg) {
        super(level, msg);
    }

    public String getStackElementClassName() {
        return this.stackElement == null ? null : this.stackElement.getClassName();
    }

    public String getStackElementMethodName() {
        return this.stackElement == null ? null : this.stackElement.getMethodName();
    }

    public String getStackElementFileName() {
        return this.stackElement == null ? null : this.stackElement.getFileName();
    }

    public int getStackLineElementNumber() {
        return this.stackElement == null ? -1 : this.stackElement.getLineNumber();
    }

    public StackTraceElement getStackElement() {
        return this.stackElement;
    }

    public void setStackElement(StackTraceElement stackElement) {
        this.stackElement = stackElement;
    }
}
