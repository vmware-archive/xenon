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

package com.vmware.workshop.labs.answer;

import static java.util.stream.Collectors.toList;

import java.util.List;

import com.vmware.workshop.labs.BookService.BookState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Hands on lab answer implementation.
 */
public class Top10BookRankingServiceAnswer extends StatelessService {
    public static final String SELF_LINK = "/app/top10";

    public static class Top10Result extends ServiceDocument {
        public List<BookState> books;
    }

    @Override
    public void handleGet(Operation get) {

        // create a query to retrieve top 10 sold books, then return Top10Result.
        // hint:
        //   - to create query: QueryTask.Query.Builder
        //   - to create query task: QueryTask.Builder
        //   - query need sorting option
        //
        //  For first, you can retrieve all data from BookService, then return only 10 from this service.
        //  Once done, you can modify query to use "setResultLimit" to restrict the number of result fetched from index.

        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(BookState.class)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(query)
                .orderDescending(BookState.FIELD_NAME_SOLD, ServiceDocumentDescription.TypeName.LONG)
//                .setResultLimit(10)
                .addOption(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT)
                .build();

        Operation.createPost(this, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                .setBody(queryTask)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        get.fail(ex);
                    }

                    QueryTask taskResult = op.getBody(QueryTask.class);
                    List<BookState> topTen = taskResult.results.documentLinks.stream()
                            .limit(10)
                            .map(link -> taskResult.results.documents.get(link))
                            .map(json -> Utils.fromJson(json, BookState.class))
                            .collect(toList());

                    Top10Result result = new Top10Result();
                    result.books = topTen;
                    get.setBodyNoCloning(result).complete();
                })
                .sendWith(this)
        ;
    }
}
