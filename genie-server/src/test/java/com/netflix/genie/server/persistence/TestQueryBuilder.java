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
 * Basic test case for the query builder utility.
 *
 * @author skrishnan
 */
public class TestQueryBuilder {

    /**
     * Test the query builder functionality.
     */
    @Test
    public void testQueryBuilder() {
        QueryBuilder qb = new QueryBuilder().table("foo").clause("a = b")
                .limit(5).page(1).desc(true).orderByUpdateTime(true);
        Assert.assertEquals(qb.getTable(), "foo");
        Assert.assertEquals(qb.getClause(), "a = b");
        Assert.assertEquals(qb.getLimit(), Integer.valueOf(5));
        Assert.assertEquals(qb.getPage(), Integer.valueOf(1));
        Assert.assertEquals(qb.isPaginate(), true);
        Assert.assertEquals(qb.isOrderByUpdateTime(), true);
    }
}
