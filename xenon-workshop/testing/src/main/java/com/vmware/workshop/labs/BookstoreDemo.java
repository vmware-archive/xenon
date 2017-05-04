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

package com.vmware.workshop.labs;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.RootNamespaceService;

public class BookstoreDemo {

    public static void main(String[] args) throws Throwable {

        // create adhoc ServiceHost.
        // In real application, you should create own ServiceHost implementation.
        ServiceHost host = ServiceHost.create();
        host.start();
        host.startDefaultCoreServicesSynchronously();
        host.startService(new RootNamespaceService());
        host.startFactory(new BookService());
        host.startService(new Top10BookRankingService());


        // Subscription
        // Let's create a subscription that will be called when BookService is created.

        // Create a consumer of subscription callbacks

        // >> IMPLEMENT HERE <<


        // start subscription when factory became available
        // For reliable subscription, use "startReliableSubscriptionService()" instead of "startSubscriptionService()"
        host.registerForServiceAvailability((o, e) -> {

            // Register subscription to the factory of BookService

            // >> IMPLEMENT HERE <<

        }, BookService.FACTORY_LINK);
    }

    private BookstoreDemo() {
    }
}
