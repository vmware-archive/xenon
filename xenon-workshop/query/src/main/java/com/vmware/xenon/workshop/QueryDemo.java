/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * StatelessService demo
 */
public class QueryDemo {

    public static void main(String[] args) throws Throwable {
        ServiceHost host = ServiceHost.create("--sandbox=/tmp/xenon-query/" + UUID.randomUUID().toString());
        host.start();
        host.startDefaultCoreServicesSynchronously();

        host.startFactory(ExampleService.class, ExampleService::createFactory);

        host.registerForServiceAvailability((o, e) -> handleFactoryAvailability(host, e),
                ExampleService.FACTORY_LINK);
    }

    private static void handleFactoryAvailability(ServiceHost host, Throwable e) {

        if (e != null) {
            host.log(Level.SEVERE, "Host failed to become available: %s", Utils.toString(e));
            return;
        }

        ExampleServiceState state1 = new ExampleServiceState();
        state1.counter = 1L;
        state1.documentSelfLink = "a";
        state1.keyValues = new HashMap<>();
        state1.keyValues.put("key1", "value1");
        state1.name = "Amanda";
        state1.sortedCounter = 1L;

        Operation post1 = Operation.createPost(host, ExampleService.FACTORY_LINK)
                .setBody(state1)
                .setReferer(host.getUri());

        ExampleServiceState state2 = new ExampleServiceState();
        state2.counter = 10L;
        state2.documentSelfLink = "b";
        state2.keyValues = new HashMap<>();
        state2.keyValues.put("key2", "value1");
        state2.name = "Bernard";
        state2.sortedCounter = 10L;

        Operation post2 = Operation.createPost(host, ExampleService.FACTORY_LINK)
                .setBody(state2)
                .setReferer(host.getUri());

        ExampleServiceState state3 = new ExampleServiceState();
        state3.counter = 100L;
        state3.documentSelfLink = "c";
        state3.keyValues = new HashMap<>();
        state3.keyValues.put("key1", "value2");
        state3.name = "Commander Adama";
        state3.sortedCounter = 100L;

        Operation post3 = Operation.createPost(host, ExampleService.FACTORY_LINK)
                .setBody(state3)
                .setReferer(host.getUri());

        OperationJoin.create(post1, post2, post3)
                .setCompletion((ops, exs) -> handlePostCompletion(host, exs))
                .sendWith(host);
    }

    private static void handlePostCompletion(ServiceHost host, Map<Long, Throwable> exs) {

        if (exs != null && !exs.isEmpty()) {
            host.log(Level.SEVERE, "Failed to start services: %s", Utils.toString(exs));
            return;
        }

        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, "Bernard",
                        QueryTask.Query.Occurance.MUST_NOT_OCCUR)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(query)
                .addOption(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT)
                .orderAscending(ExampleServiceState.FIELD_NAME_SORTED_COUNTER,
                        ServiceDocumentDescription.TypeName.LONG)
                .build();

        Operation post = Operation.createPost(host, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(queryTask)
                .setReferer(host.getUri())
                .setCompletion((o, e) -> handleQueryCompletion(host, o, e));

        host.sendRequest(post);
    }

    private static void handleQueryCompletion(ServiceHost host, Operation o, Throwable e) {

        if (e != null) {
            host.log(Level.SEVERE, "Query task failed: %s", Utils.toString(e));
            return;
        }

        QueryTask queryTask = o.getBody(QueryTask.class);
        if (queryTask.results.documentLinks.isEmpty()) {
            host.log(Level.INFO, "Query returned no results");
            return;
        }

        host.log(Level.INFO, "Returned query results: %s", Utils.toJsonHtml(queryTask.results));
    }

    private QueryDemo() {
    }
}
