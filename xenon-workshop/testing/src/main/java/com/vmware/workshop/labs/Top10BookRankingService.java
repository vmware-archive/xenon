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

import java.util.List;

import com.vmware.workshop.labs.BookService.BookState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;

public class Top10BookRankingService extends StatelessService {
    public static final String SELF_LINK = "/app/top10";

    public static class Top10Result extends ServiceDocument {
        public List<BookState> books;
    }

    @Override
    public void handleGet(Operation get) {

        // Create a query to retrieve top 10 sold books, then return Top10Result.
        // hint:
        //   - to create query: QueryTask.Query.Builder
        //   - to create query task: QueryTask.Builder
        //   - query need sorting option
        //
        //  For first, you can retrieve all data from BookService, then return only 10 from this service.
        //  Once done, you can modify query to use "setResultLimit" to restrict the number of result fetched from index.

        // >> IMPLEMENT HERE <<

    }
}
