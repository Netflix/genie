/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.services.impl.jpa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import javax.persistence.metamodel.SingularAttribute;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility methods for working with JPA and Spring.
 *
 * @author tgianos
 */
public final class JPAUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigServiceJPAImpl.class);

    /**
     * Private constructor for Utility class to prevent instantiation.
     */
    private JPAUtils() {
    }

    /**
     * Get a page request to be used when querying the database which sets the page, limit, order direction and
     * order by parameters for querying the databases.
     *
     * @param page The page to get starting with index 0. page * limit = first element returned.
     * @param limit The number of elements to return after the starting element.
     * @param descending Whether the order should be descending or ascending.
     * @param orderBys The fields to order the results by.
     * @param entityMetaModelClass The class of the entity we're evaluating against.
     * @param defaultField The default order by field to use.
     * @return The page request to use.
     */
    public static PageRequest getPageRequest(
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys,
            final Class<?> entityMetaModelClass,
            final String defaultField
    ) {
        final List<String> finalOrderBys = new ArrayList<>();

        if (orderBys != null) {
            for (final String fieldName : orderBys) {
                try {
                    final Field field = entityMetaModelClass.getField(fieldName);
                    //The field exists but is it a singular attribute?
                    if (field.getType() == SingularAttribute.class) {
                        finalOrderBys.add(fieldName);
                    } else if (LOG.isDebugEnabled()) {
                        LOG.debug("Field " + fieldName + " is a collection and can't be used for order by.");
                    }
                } catch (final NoSuchFieldException nsfe) {
                    //Swallow and ignore
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No such field " + fieldName + ". " + nsfe.getMessage());
                    }
                }
            }
        }

        if (finalOrderBys.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No valid order by parameters set. Using default field " + defaultField);
            }
            finalOrderBys.add(defaultField);
        }

        return new PageRequest(
                page < 0 ? 0 : page,
                limit < 1 ? 1024 : limit,
                descending ? Sort.Direction.DESC : Sort.Direction.ASC,
                finalOrderBys.toArray(new String[finalOrderBys.size()])
        );
    }
}
