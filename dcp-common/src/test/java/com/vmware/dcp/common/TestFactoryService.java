/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.test.VerificationHost;

public class TestFactoryService {

    public static final String FAC_PATH = "/subpath/fff";

    private VerificationHost host;
    private URI factoryUri;

    @Before
    public void setup() throws Throwable {
        this.host = VerificationHost.create(0, null);
        this.host.start();

        this.factoryUri = UriUtils.buildUri(this.host, SomeFactoryService.class);

    }

    @After
    public void tearDown() throws Throwable {
        this.host.stop();
    }

    @Test
    public void testFactoryPostHandling() throws Throwable {
        this.host.startService(
                Operation.createPost(this.factoryUri),
                new SomeFactoryService());
        this.host.waitForServiceAvailable(SomeFactoryService.SELF_LINK);

        this.host.testStart(4);
        idempotentPostReturnsUpdatedOpBody();
        checkDerivedSelfLinkWhenProvidedSelfLinkIsJustASuffix();
        checkDerivedSelfLinkWhenProvidedSelfLinkAlreadyContainsAPath();
        checkDerivedSelfLinkWhenProvidedSelfLinkLooksLikeItContainsAPathButDoesnt();
        this.host.testWait();
    }

    @Test
    public void postFactoryQueueing() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath";

        this.host.testStart(1);
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.factoryUri))
                .setBody(doc)
                .setCompletion(
                        (op, ex) -> {
                            if (op.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_NOT_FOUND"));
                        });

        this.host.send(post);
        this.host.testWait();

        this.host.testStart(2);
        post = Operation
                .createPost(this.factoryUri)
                .setBody(doc)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                .setCompletion(
                        (op, ex) -> {
                            if (op.getStatusCode() == Operation.STATUS_CODE_OK) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_OK"));
                        });
        this.host.send(post);
        this.host.startService(
                Operation.createPost(this.factoryUri),
                new SomeFactoryService());
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                SomeFactoryService.SELF_LINK);
        this.host.testWait();

    }

    private void idempotentPostReturnsUpdatedOpBody() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath/fff/apple";
        doc.value = 2;

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            this.host.send(Operation.createPost(this.factoryUri)
                                    .setBody(doc)
                                    .setCompletion(
                                            (o2, e2) -> {
                                                if (e2 != null) {
                                                    this.host.failIteration(e2);
                                                    return;
                                                }

                                                SomeDocument doc2 = o2.getBody(SomeDocument.class);
                                                try {
                                                    assertNotNull(doc2);
                                                    assertEquals(4, doc2.value);
                                                    this.host.completeIteration();
                                                } catch (AssertionError e3) {
                                                    this.host.failIteration(e3);
                                                }
                                            }));
                        }));
    }

    private void checkDerivedSelfLinkWhenProvidedSelfLinkIsJustASuffix() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "freddy/x1";

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    String selfLink = o.getBody(SomeDocument.class).documentSelfLink;
                    URI opUri = o.getUri();

                    String expectedPath = "/subpath/fff/freddy/x1";
                    try {
                        assertEquals(expectedPath, selfLink);
                        assertEquals(UriUtils.buildUri(this.host, expectedPath), opUri);
                        this.host.completeIteration();
                    } catch (Throwable e2) {
                        this.host.failIteration(e2);
                    }
                }));
    }

    private void checkDerivedSelfLinkWhenProvidedSelfLinkAlreadyContainsAPath() throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath/fff/freddy/x2";

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    String selfLink = o.getBody(SomeDocument.class).documentSelfLink;
                    URI opUri = o.getUri();

                    String expectedPath = "/subpath/fff/freddy/x2";
                    try {
                        assertEquals(expectedPath, selfLink);
                        assertEquals(UriUtils.buildUri(this.host, expectedPath), opUri);
                        this.host.completeIteration();
                    } catch (Throwable e2) {
                        this.host.failIteration(e2);
                    }
                }));
    }

    private void checkDerivedSelfLinkWhenProvidedSelfLinkLooksLikeItContainsAPathButDoesnt()
            throws Throwable {
        SomeDocument doc = new SomeDocument();
        doc.documentSelfLink = "/subpath/fffreddy/x3";

        this.host.send(Operation.createPost(this.factoryUri)
                .setBody(doc)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    String selfLink = o.getBody(SomeDocument.class).documentSelfLink;
                    URI opUri = o.getUri();

                    String expectedPath = "/subpath/fff/subpath/fffreddy/x3";
                    try {
                        assertEquals(expectedPath, selfLink);
                        assertEquals(UriUtils.buildUri(this.host, expectedPath), opUri);
                        this.host.completeIteration();
                    } catch (Throwable e2) {
                        this.host.failIteration(e2);
                    }
                }));
    }

    public static class SomeFactoryService extends FactoryService {

        public static final String SELF_LINK = FAC_PATH;

        SomeFactoryService() {
            super(SomeDocument.class);
            toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new SomeStatefulService();
        }

    }

    public static class SomeStatefulService extends StatefulService {

        SomeStatefulService() {
            super(SomeDocument.class);
        }

        @Override
        public void handlePut(Operation put) {
            SomeDocument a = put.getBody(SomeDocument.class);
            SomeDocument b = new SomeDocument();
            a.copyTo(b);
            b.value = 2 + a.value;
            put.setBody(b).complete();
        }

    }

    public static class SomeDocument extends ServiceDocument {

        public int value;

    }

}
