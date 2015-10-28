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
import static org.junit.Assert.fail;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Service.Action;

public class TestRequestRouter {

    private RequestRouter router;
    private int xCount;
    private int yCount;

    @Before
    public void setUp() throws Exception {
        this.router = new RequestRouter();
        this.xCount = 0;
        this.yCount = 0;
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() throws Exception {
        int NUM = 10;

        this.router.register(Action.PATCH, new RequestRouter.RequestUriMatcher("action=doX"),
                this::doX, "perform X");
        this.router.register(Action.PATCH, new RequestRouter.RequestUriMatcher("action=doY"),
                this::doY, "perform Y");

        for (int i = 0; i < NUM; i++) {
            String uri = i % 2 == 0 ? "http://localhost/?action=doX"
                    : "http://localhost/?action=doY";
            if (this.router.test(Operation.createPatch(new URI(uri)))) {
                fail("route not found");
            }
        }

        assertEquals(NUM / 2, this.xCount);
        assertEquals(NUM / 2, this.yCount);
    }

    private void doX(Operation op) {
        this.xCount++;
    }

    private void doY(Operation op) {
        this.yCount++;
    }
}
