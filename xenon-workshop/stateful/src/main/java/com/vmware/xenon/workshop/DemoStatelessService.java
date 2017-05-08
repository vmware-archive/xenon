/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.workshop;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

public class DemoStatelessService extends StatelessService {

    // This field is read by the service host to determine the self-link of a
    // service instance at startup time (if one is not specified).
    public static String SELF_LINK = "/demo/stateless";

    @Override
    public void handlePost(Operation post) {
        logInfo("Received POST operation with body '%s'", post.getBody(String.class));
        post.setStatusCode(Operation.STATUS_CODE_CREATED).complete();
    }

    @Override
    public void handlePut(Operation put) {
        logInfo("Received PUT operation with body '%s'", put.getBody(String.class));
        put.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        logInfo("Received PATCH operation with body '%s'", patch.getBody(String.class));
        patch.complete();
    }

    @Override
    public void handleGet(Operation get) {
        logInfo("Received GET operation");
        get.setBody("Hello world").complete();
    }
}
