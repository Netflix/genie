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

import java.net.HttpURLConnection;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Builder class to generate query criteria or set statement.
 *
 * @author skrishnan
 */
public class ClauseBuilder {

    /**
     * Represents an "and" in a query.
     */
    public static final String AND = " and ";

    /**
     * Represents an "or" in a query.
     */
    public static final String OR = " or ";

    /**
     * Represents a comma (",") in a query.
     */
    public static final String COMMA = ", ";

    // builder, which stores the criteria thus far
    private StringBuilder sb = new StringBuilder();

    // if the criteria was empty prior to method invocation
    private boolean wasEmpty = true;

    // the conjunction value for this builder
    private String conjunction;

    // this can be either empty or " and/or/, ", depending on whether the
    // criteria is empty or not
    private String currConjunction;

    // order by clause
    private String orderBy;

    /**
     * Constructor for the criteria builder.
     *
     * @param conjunction
     *            one of ClauseBuilder.AND or ClauseBuilder.OR
     * @throws CloudServiceException
     *             if an illegal conjunction is sent
     */
    public ClauseBuilder(String conjunction) throws CloudServiceException {
        // basic error checking
        if (!((conjunction.equalsIgnoreCase(AND))
                || (conjunction.equalsIgnoreCase(OR)) || (conjunction
                    .equalsIgnoreCase(COMMA)))) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Bad value for conjunction: " + conjunction
                            + ", should be {' and ', ' or ', ' , '}");
        }
        this.conjunction = conjunction;

        // initialize current conjunction to empty, until first entry has been
        // appended
        this.currConjunction = "";

    }

    /**
     * Append a clause to the criteria thus far.
     *
     * @param clause
     *            simple clause of the form name=value or name='value'<br>
     *            name=value will be converted to T.name=value
     */
    public void append(String clause) {
        append(clause, true);
    }

    /**
     * Any clause to append to query, with or without an alias.
     *
     * @param clause
     *            of the form a=b or T.a=b, etc
     * @param withAlias
     *            if true, "T." will the prepended to the clause. Otherwise
     *            clause will be used as is.
     */
    public void append(String clause, boolean withAlias) {
        // basic error checking
        if ((clause == null) || (clause.isEmpty())) {
            return;
        }

        // append to the existing criteria
        sb.append(currConjunction);
        if (withAlias) {
            sb.append(PersistenceManager.ENTITY_ALIAS);
            sb.append(".");
            sb.append(clause);
        } else {
            sb.append(clause);
        }

        // change the conjunction to and, if this was the first entry
        if (wasEmpty) {
            currConjunction = conjunction;
            wasEmpty = false;
        }
    }

    /**
     * Add an order by clause at the very end of the query.
     *
     * @param clause
     *            something like "X asc". A "T." will be prepended to the
     *            clause.
     */
    public void setOrderBy(String clause) {
        setOrderBy(clause, true);
    }

    /**
     * Add an order by clause at the very end of the query.
     *
     * @param clause
     *            something like "X asc"
     * @param withAlias
     *            if set to true, it will prepend T. to clause. Otherwise,
     *            clause will be used as is
     */
    public void setOrderBy(String clause, boolean withAlias) {
        if (withAlias) {
            orderBy = "order by " + PersistenceManager.ENTITY_ALIAS + "."
                    + clause;
        } else {
            orderBy = "order by " + clause;
        }
    }

    /**
     * Return string representation of this query.
     */
    @Override
    public String toString() {
        if (orderBy != null) {
            return sb.toString() + " " + orderBy;
        } else {
            return sb.toString();
        }
    }
}
