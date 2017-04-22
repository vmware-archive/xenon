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

package com.vmware.xenon.services.common;

import java.util.Collections;
import java.util.EnumSet;

import com.vmware.xenon.common.ODataQueryVisitor.BinaryVerb;
import com.vmware.xenon.common.ODataToken;
import com.vmware.xenon.common.ODataTokenList;
import com.vmware.xenon.common.ODataTokenizer;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

public class RootNamespaceService extends StatelessService {
    public static final String SELF_LINK = "/";

    public RootNamespaceService() {
        super();
    }

    @Override
    public void handleGet(Operation get) {
        EnumSet<ServiceOption> options = EnumSet.of(ServiceOption.FACTORY);

        String filter = UriUtils.getODataFilterParamValue(get.getUri());
        if (filter != null) {
            ODataTokenizer tokenizer = new ODataTokenizer(filter);
            ODataTokenList list = tokenizer.tokenize();

            while (list.hasNext()) {
                ODataToken tok = list.next();
                if (!tok.getUriLiteral().equals("options")) {
                    continue;
                }

                if (list.hasNext() && list.next().getUriLiteral().equals(BinaryVerb.EQ.operator)) {
                    String value = null;
                    if (list.hasNext()) {
                        value = list.next().getUriLiteral();
                    }

                    if ("STATELESS".equals(value) || "'STATELESS'".equals(value)) {
                        options.add(ServiceOption.STATELESS);
                        break;
                    }
                }
            }
        }

        get.nestCompletion((o, e) -> {
            if (e == null) {
                Collections.sort(o.getBody(ServiceDocumentQueryResult.class).documentLinks);
            }

            get.complete();
        });

        getHost().queryServiceUris(options, false, get);
    }
}
