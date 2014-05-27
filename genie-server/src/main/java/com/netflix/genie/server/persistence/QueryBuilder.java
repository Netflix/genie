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

/**
 * A builder class, which helps simplify building of complex queries.<br>
 * Defaults are: paginate=true, page=0, orderByUpdateTime=true, desc=true, and
 * limit=PersistenceManager.MAX_PAGE_SIZE.
 *
 * @author skrishnan
 */
public class QueryBuilder {

    // table name for the query
    private String table;

    // the set statement for the query
    private String set;

    // predicate clause to use for the query
    private String clause;

    // paginate or not, defaults to true
    private boolean paginate = true;

    // page number, defaults to 0th page
    private Integer page = 0;

    // number of items to return (per page), defaults to max page size
    private Integer limit = PersistenceManager.MAX_PAGE_SIZE;

    // whether to enforce any ordering by updateTime or not, defaults to true
    private boolean orderByUpdateTime = true;

    // descending or ascending order, in terms of updateTime
    // defaults to descending order on updateTime
    private boolean desc = true;

    /**
     * Get the table name for the query.
     *
     * @return table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Set the table name for query.
     *
     * @param table
     *            element/table name to use for the query
     * @return current (this) builder object
     */
    public QueryBuilder table(String table) {
        this.table = table;
        return this;
    }

    /**
     * Get the set statement for this query.
     *
     * @return set statement for query
     */
    public String getSet() {
        return set;
    }

    /**
     * Set the set statement for this query.
     *
     * @param set
     *            set statement
     * @return current (this) builder object
     */
    public QueryBuilder set(String set) {
        this.set = set;
        return this;
    }

    /**
     * Get the clause for the query.
     *
     * @return clause
     */
    public String getClause() {
        return clause;
    }

    /**
     * Set the clause for the query.
     *
     * @param clause
     *            the clause to use
     * @return current (this) builder object
     */
    public QueryBuilder clause(String clause) {
        this.clause = clause;
        return this;
    }

    /**
     * Get number of objects to return.
     *
     * @return limit
     */
    public Integer getLimit() {
        return limit;
    }

    /**
     * Set the number of objects to return.
     *
     * @param limit
     *            number of objects to return
     * @return current (this) builder object
     */
    public QueryBuilder limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Get page number for the query.
     *
     * @return page number
     */
    public Integer getPage() {
        return page;
    }

    /**
     * Set page number for query.
     *
     * @param page
     *            page number to start from
     * @return current (this) builder object
     */
    public QueryBuilder page(Integer page) {
        this.page = page;
        return this;
    }

    /**
     * Should results be ordered by update time?
     *
     * @return whether results are to be ordered by update time
     */
    public boolean isOrderByUpdateTime() {
        return orderByUpdateTime;
    }

    /**
     * Should results be ordered by update time - default is true.
     *
     * @param orderByUpdateTime
     *            whether results are to be ordered by update time
     * @return current (this) builder object
     */
    public QueryBuilder orderByUpdateTime(boolean orderByUpdateTime) {
        this.orderByUpdateTime = orderByUpdateTime;
        return this;
    }

    /**
     * Is the order by descending?
     *
     * @return true, if order by is descending
     */
    public boolean isDesc() {
        return desc;
    }

    /**
     * Is the order by descending?
     *
     * @param desc
     *            true, if order by is descending
     * @return current (this) builder object
     */
    public QueryBuilder desc(boolean desc) {
        this.desc = desc;
        return this;
    }

    /**
     * Should we paginate?
     *
     * @return true, if pagination is desired
     */
    public boolean isPaginate() {
        return paginate;
    }

    /**
     * Should we paginate?
     *
     * @param paginate
     *            true, if pagination is desired
     * @return current (this) builder object
     */
    public QueryBuilder paginate(boolean paginate) {
        this.paginate = paginate;
        return this;
    }
}
