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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.workshop.labs.BookService.BookState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

public class BookServiceTest {

    private VerificationHost host;

    @Before
    public void setUp() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.start();

        // start services
        this.host.startFactory(new BookService());
        this.host.startService(new Top10BookRankingService());

        this.host.waitForServiceAvailable(BookService.FACTORY_LINK, Top10BookRankingService.SELF_LINK);
    }

    @After
    public void tearDown() {
        this.host.tearDown();
        this.host = null;
    }

    @Test
    public void basic() throws Throwable {
        BookState bookFoo = createBook("foo", 10);
        BookState bookBar = createBook("bar", 7);

        List<Operation> posts = new ArrayList<>();
        posts.add(Operation.createPost(this.host, BookService.FACTORY_LINK).setBody(bookFoo));
        posts.add(Operation.createPost(this.host, BookService.FACTORY_LINK).setBody(bookBar));

        TestRequestSender sender = this.host.getTestRequestSender();
        sender.sendAndWait(posts);

        String linkForFoo = UriUtils.buildUriPath(BookService.FACTORY_LINK, "foo");
        Operation get = Operation.createGet(this.host, linkForFoo);
        BookState book = sender.sendAndWait(get, BookState.class);
        assertEquals("foo", book.title);
        assertEquals(10, book.sold);
    }

    private BookState createBook(String title, int sold) {
        BookState book = new BookState();
        book.title = title;
        book.sold = sold;
        book.documentSelfLink = title;
        return book;
    }

    @Test
    public void top10() {

        // Create a test to verify top10 docs
        //  - populate more than 10 documents
        //  - call top10 endpoint
        //  - verify top10 books in response
        //
        //  Once done, you can try with multinode environment

        // >> IMPLEMENT HERE <<

    }
}
