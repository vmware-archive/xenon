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
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.RootNamespaceService;

public class Demo {

    public static void main(String[] args) throws Throwable {

        ServiceHost host = ServiceHost.create("--sandbox=/tmp/xenon-demo");
        host.start();
        host.startDefaultCoreServicesSynchronously();

        // A simple stateless service which enumerates all the factory services started on the service host.
        host.startService(new RootNamespaceService());

        // A simple stateless service.
        host.startService(new DemoStatelessService());

        // A simple stateful service.
        DemoStatefulService.State initialState = new DemoStatefulService.State();
        initialState.stringValue = "A string value";
        Operation post = Operation.createPost(host, DemoStatefulService.SELF_LINK).setBody(initialState);
        host.startService(post, new DemoStatefulService());

        // A simple factory service.
        host.startService(new DemoFactoryService());
    }
}
