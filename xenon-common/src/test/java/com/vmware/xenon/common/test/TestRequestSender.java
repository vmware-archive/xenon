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

package com.vmware.xenon.common.test;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRequestSender;

/**
 * Provides synchronous/asynchronous send operations for test.
 */
public class TestRequestSender {

    public static class FailureResponse {
        public Operation op;
        public Throwable failure;
    }

    private ServiceRequestSender sender;
    private Duration timeout = Duration.ofSeconds(30);
    private URI referer;

    public TestRequestSender(ServiceRequestSender sender) {
        this.sender = sender;

        if (this.sender instanceof VerificationHost) {
            this.timeout = Duration.ofSeconds(((VerificationHost) this.sender).getTimeoutSeconds());
        }
    }

    /**
     * Asynchronously perform operation
     */
    public void sendRequest(Operation op) {
        // TODO: populate caller stack information to op, when op fails, add it to suppressed exception

        URI referer = this.referer;
        if (referer == null) {
            // populate referer from current stack information
            Optional<StackTraceElement> stackElement = Arrays.stream(Thread.currentThread().getStackTrace())
                    .filter(elem -> {
                        // filter out Thread and this class
                        String className = elem.getClassName();
                        return !(Thread.class.getName().equals(className) || getClass().getName().equals(className));
                    }).findFirst();

            String refererString;
            if (stackElement.isPresent()) {
                StackTraceElement elem = stackElement.get();
                refererString = String.format("http://localhost/%s?line=%s&class=%s&method=%s",
                        getClass().getSimpleName(), elem.getLineNumber(), elem.getClassName(), elem.getMethodName());
            } else {
                refererString = String.format("http://localhost/%s@%s", getClass().getSimpleName(), this.hashCode());
            }
            referer = URI.create(refererString);
        }
        op.setReferer(referer);
        this.sender.sendRequest(op);
    }

    /**
     * Synchronously perform operation.
     *
     * Expecting provided {@link Operation} to be successful.
     * {@link Operation.CompletionHandler} does NOT need explicitly pass/use of {@link TestContext}.
     *
     * @param op       operation to perform
     * @param bodyType returned body type
     * @param <T>      ServiceDocument
     * @return body document
     */
    public <T extends ServiceDocument> T sendAndWait(Operation op, Class<T> bodyType) {
        Operation response = sendAndWait(op);
        return response.getBody(bodyType);
    }

    /**
     * Synchronously perform operation.
     *
     * Expecting provided operation to be successful.
     * {@link Operation.CompletionHandler} does NOT need explicitly pass/use of {@link TestContext}.
     *
     * @param op operation to perform
     * @return callback operation
     */
    public Operation sendAndWait(Operation op) {
        Operation[] response = new Operation[1];

        TestContext waitContext = new TestContext(1, this.timeout);
        op.appendCompletion((o, e) -> {
            if (e != null) {
                waitContext.fail(e);
                return;
            }
            response[0] = o;
            waitContext.complete();
        });
        sendRequest(op);
        waitContext.await();

        return response[0];
    }

    /**
     * Synchronously perform operation.
     *
     * Expecting provided operation to be failed.
     *
     * @param op operation to perform
     * @return callback operation and failure
     */
    public FailureResponse sendAndWaitFailure(Operation op) {
        FailureResponse response = new FailureResponse();

        TestContext waitContext = new TestContext(1, this.timeout);
        op.appendCompletion((o, e) -> {
            if (e != null) {
                response.op = o;
                response.failure = e;
                waitContext.complete();
                return;
            }
            String msg = String.format("Expected operation failure but was successful. uri=%s", o.getUri());
            waitContext.fail(new RuntimeException(msg));
        });
        sendRequest(op);
        waitContext.await();

        return response;
    }

    private ServiceHost getSender() {

        if (this.sender instanceof ServiceHost) {
            return (ServiceHost) this.sender;
        } else if (this.sender instanceof Service) {
            return ((Service) this.sender).getHost();
        }

        throw new UnsupportedOperationException("Not supported to " + this.sender);
    }

    public Duration getTimeout() {
        return this.timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public URI getReferer() {
        return this.referer;
    }

    public void setReferer(URI referer) {
        this.referer = referer;
    }
}
