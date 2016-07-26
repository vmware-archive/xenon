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

package com.vmware.xenon.examples;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * An example service. Although it is a Xenon service, it should not be
 * dynamically loaded as it does not define a SELF_LINK or FACTORY_LINK.
 */
public class ExampleFooService extends StatefulService {
    public static class ExampleFooServiceState extends ServiceDocument {
        public String name;
    }

    public ExampleFooService() {
        super(ExampleFooServiceState.class);
    }
}
