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
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

public class DemoStatefulService extends StatefulService {

    // This field is read by the service host to determine the self-link of a
    // service instance at startup time (if one is not specified).
    public static String SELF_LINK = "/demo/stateful";

    public static class State extends ServiceDocument {
        public String stringValue;
        public Long longValue;
    }

    public DemoStatefulService() {
        super(State.class);
    }

    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        // Get the body of the PUT operation and validate it.
        State putState = put.getBody(State.class);
        if (putState.stringValue == null && putState.longValue == null) {
            put.fail(new IllegalArgumentException("One of stringValue and longValue is required"));
            return;
        }

        // Update the current state of the service instance with the new state.
        setState(put, putState);

        // Complete the operation and return.
        put.complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        // Get the body of the PATCH operation and validate it.
        State patchState = patch.getBody(State.class);
        if (patchState.stringValue == null && patchState.longValue == null) {
            patch.fail(new IllegalArgumentException("One of stringValue and longValue is required"));
            return;
        }

        // Get the current state of the service instance and apply the updates.
        State currentState = getState(patch);

        if (patchState.stringValue != null) {
            currentState.stringValue = patchState.stringValue;
        }

        if (patchState.longValue != null) {
            currentState.longValue = patchState.longValue;
        }

        // Complete the operation and return.
        patch.complete();
    }
}
