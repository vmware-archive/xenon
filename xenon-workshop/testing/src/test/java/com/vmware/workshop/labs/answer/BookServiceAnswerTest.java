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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.workshop.labs.BookService;
import com.vmware.workshop.labs.BookService.BookState;
import com.vmware.workshop.labs.answer.Top10BookRankingServiceAnswer.Top10Result;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

/**
 * Hands on lab answer implementation.
 */
public class BookServiceAnswerTest {


    private VerificationHost host;

    @Before
    public void setUp() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.start();

        // start services
        this.host.startFactory(new BookService());
        this.host.startService(new Top10BookRankingServiceAnswer());

        this.host.waitForServiceAvailable(BookService.FACTORY_LINK, Top10BookRankingServiceAnswer.SELF_LINK);
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
        //  Once done, you can try with multinode environment.

        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            BookState book = createBook("foo-" + i, i * 10);
            Operation post = Operation.createPost(this.host, BookService.FACTORY_LINK).setBody(book);
            posts.add(post);
        }

        TestRequestSender sender = this.host.getTestRequestSender();
        sender.sendAndWait(posts);

        Operation get = Operation.createGet(this.host, Top10BookRankingServiceAnswer.SELF_LINK);
        Top10Result topTen = sender.sendAndWait(get, Top10Result.class);
        assertEquals(10, topTen.books.size());
        assertEquals("foo-19", topTen.books.get(0).title);
        assertEquals("foo-18", topTen.books.get(1).title);
        assertEquals("foo-17", topTen.books.get(2).title);
        assertEquals("foo-16", topTen.books.get(3).title);
        assertEquals("foo-15", topTen.books.get(4).title);
        assertEquals("foo-14", topTen.books.get(5).title);
        assertEquals("foo-13", topTen.books.get(6).title);
        assertEquals("foo-12", topTen.books.get(7).title);
        assertEquals("foo-11", topTen.books.get(8).title);
        assertEquals("foo-10", topTen.books.get(9).title);

    }
}
