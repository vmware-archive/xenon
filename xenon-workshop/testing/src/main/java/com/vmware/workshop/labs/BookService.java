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

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

public class BookService extends StatefulService {

    public static final String FACTORY_LINK = "/data/books";

    public static class BookState extends ServiceDocument {
        public static final String FIELD_NAME_TITLE = "title";
        public static final String FIELD_NAME_SOLD = "sold";

        // book title
        public String title;

        // number of books sold
        public int sold;
    }

    public BookService() {
        super(BookState.class);
        this.toggleOption(ServiceOption.PERSISTENCE, true);
        this.toggleOption(ServiceOption.OWNER_SELECTION, true);
        this.toggleOption(ServiceOption.REPLICATION, true);
    }
}
