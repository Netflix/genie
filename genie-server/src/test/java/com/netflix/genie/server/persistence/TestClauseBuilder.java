/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.genie.server.persistence;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for the clause builder class.
 *
 * @author skrishnan
 */
public class TestClauseBuilder {

    /**
     * Test bad conjunction in query - something other than AND, OR or ",".
     */
    @Test
    public void testBadConjunction() {
        try {
            new ClauseBuilder("FOO");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    /**
     * Test a simply query that doesn't contain any nested clauses.
     */
    @Test
    public void testSimpleQuery() {
        try {
            ClauseBuilder cb = new ClauseBuilder(ClauseBuilder.AND);
            cb.append("a = b");
            cb.append("c = d");
            cb.setOrderBy("e desc");
            Assert.assertEquals(cb.toString(),
                    "T.a = b and T.c = d order by T.e desc");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test simply query with a custom alias (i.e. not T, which is the default).
     */
    @Test
    public void testSimpleQueryWithAlias() {
        try {
            ClauseBuilder cb = new ClauseBuilder(ClauseBuilder.AND);
            cb.append("X.a = b", false);
            cb.append("X.c = d", false);
            Assert.assertEquals(cb.toString(), "X.a = b and X.c = d");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test a complex query with nested clauses.
     */
    @Test
    public void testComplexQuery() {
        try {
            ClauseBuilder cb0 = new ClauseBuilder(ClauseBuilder.AND);
            cb0.append("a = b");
            ClauseBuilder cb1 = new ClauseBuilder(ClauseBuilder.OR);
            cb1.append("c = d");
            cb1.append("e = f");
            cb0.append("(" + cb1.toString() + ")", false);
            Assert.assertEquals(cb0.toString(),
                    "T.a = b and (T.c = d or T.e = f)");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test an update query, which includes a "set".
     */
    @Test
    public void testSet() {
        try {
            ClauseBuilder cb = new ClauseBuilder(ClauseBuilder.COMMA);
            cb.append("a = b");
            cb.append("c = d");
            cb.append("T.e = f", false);
            Assert.assertEquals(cb.toString(), "T.a = b, T.c = d, T.e = f");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
