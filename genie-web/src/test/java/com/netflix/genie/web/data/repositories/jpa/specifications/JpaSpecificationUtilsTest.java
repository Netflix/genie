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
package com.netflix.genie.web.data.repositories.jpa.specifications;

import com.google.common.collect.Sets;
import com.netflix.genie.web.data.entities.TagEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;
import java.util.Set;

/**
 * Unit tests for JpaSpecificationUtils.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JpaSpecificationUtilsTest {

    /**
     * Make sure the method to create the tag search string for jobs is working as expected.
     */
    @Test
    void canCreateTagSearchString() {
        final String one = "oNe";
        final String two = "TwO";
        final String three = "3";
        final Set<TagEntity> tags = Sets.newHashSet();
        Assertions
            .assertThat(JpaSpecificationUtils.createTagSearchString(tags))
            .isEqualTo(
                // Coerce to string... sigh
                JpaSpecificationUtils.TAG_DELIMITER + JpaSpecificationUtils.TAG_DELIMITER
            );

        final TagEntity oneTag = new TagEntity();
        oneTag.setTag(one);

        final TagEntity twoTag = new TagEntity();
        twoTag.setTag(two);

        final TagEntity threeTag = new TagEntity();
        threeTag.setTag(three);

        tags.add(oneTag);
        Assertions
            .assertThat(JpaSpecificationUtils.createTagSearchString(tags))
            .isEqualTo(
                JpaSpecificationUtils.TAG_DELIMITER
                    + one
                    + JpaSpecificationUtils.TAG_DELIMITER
            );

        tags.add(twoTag);
        Assertions.assertThat(JpaSpecificationUtils.createTagSearchString(tags))
            .isEqualTo(
                JpaSpecificationUtils.TAG_DELIMITER
                    + one
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + two
                    + JpaSpecificationUtils.TAG_DELIMITER
            );

        tags.add(threeTag);
        Assertions.assertThat(JpaSpecificationUtils.createTagSearchString(tags))
            .isEqualTo(
                JpaSpecificationUtils.TAG_DELIMITER
                    + three
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + one
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + two
                    + JpaSpecificationUtils.TAG_DELIMITER
            );
    }

    /**
     * Make sure if a string parameter contains a % it returns a like predicate but if not it returns an equals
     * predicate.
     */
    @SuppressWarnings("unchecked")
    @Test
    void canGetStringLikeOrEqualPredicate() {
        final CriteriaBuilder cb = Mockito.mock(CriteriaBuilder.class);
        final Expression<String> expression = (Expression<String>) Mockito.mock(Expression.class);
        final Predicate likePredicate = Mockito.mock(Predicate.class);
        final Predicate equalPredicate = Mockito.mock(Predicate.class);
        Mockito.when(cb.like(Mockito.eq(expression), Mockito.anyString())).thenReturn(likePredicate);
        Mockito.when(cb.equal(Mockito.eq(expression), Mockito.anyString())).thenReturn(equalPredicate);

        Assertions
            .assertThat(JpaSpecificationUtils.getStringLikeOrEqualPredicate(cb, expression, "equal"))
            .isEqualTo(equalPredicate);
        Assertions
            .assertThat(JpaSpecificationUtils.getStringLikeOrEqualPredicate(cb, expression, "lik%e"))
            .isEqualTo(likePredicate);
    }

    /**
     * Make sure we can get a valid like string for the tag list.
     */
    @Test
    void canGetTagLikeString() {
        Assertions
            .assertThat(JpaSpecificationUtils.getTagLikeString(Sets.newHashSet()))
            .isEqualTo(
                // coerce to String
                JpaSpecificationUtils.PERCENT
            );
        Assertions
            .assertThat(JpaSpecificationUtils.getTagLikeString(Sets.newHashSet("tag")))
            .isEqualTo(
                JpaSpecificationUtils.PERCENT
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + "tag"
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + JpaSpecificationUtils.PERCENT
            );
        Assertions
            .assertThat(JpaSpecificationUtils.getTagLikeString(Sets.newHashSet("tag", "Stag", "rag")))
            .isEqualTo(
                JpaSpecificationUtils.PERCENT
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + "rag"
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + "%"
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + "Stag"
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + "%"
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + "tag"
                    + JpaSpecificationUtils.TAG_DELIMITER
                    + JpaSpecificationUtils.PERCENT
            );
    }
}
