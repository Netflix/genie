/*
 * Copyright 2014 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package com.netflix.genie.server.repository.jpa;

import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Application_;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for the application specifications.
 *
 * @author tgianos
 */
public class TestApplicationSpecs {

    private static final String NAME = "tez";
    private static final String USER_NAME = "tgianos";
    private static final String TAG_1 = "tez";
    private static final String TAG_2 = "yarn";
    private static final String TAG_3 = "hadoop";
    private static final Set<String> TAGS = new HashSet<>();

    private Root<Application> root;
    private CriteriaQuery<?> cq;
    private CriteriaBuilder cb;

    /**
     * Setup test wide variables.
     */
    @BeforeClass
    public static void setupClass() {
        TAGS.add(TAG_1);
        TAGS.add(TAG_2);
        TAGS.add(TAG_3);
    }

    /**
     * Setup some variables.
     */
    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        this.root = (Root<Application>) Mockito.mock(Root.class);
        this.cq = Mockito.mock(CriteriaQuery.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);

        final Path<String> commandNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Application_.name)).thenReturn(commandNamePath);
        Mockito.when(this.cb.equal(commandNamePath, NAME))
                .thenReturn(equalNamePredicate);

        final Path<String> userNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalUserNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Application_.user)).thenReturn(userNamePath);
        Mockito.when(this.cb.equal(userNamePath, USER_NAME))
                .thenReturn(equalUserNamePredicate);

        final Expression<Set<String>> tagExpression = (Expression<Set<String>>) Mockito.mock(Expression.class);
        final Predicate isMemberTagPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Application_.tags)).thenReturn(tagExpression);
        Mockito.when(this.cb.isMember(Mockito.any(String.class), Mockito.eq(tagExpression)))
                .thenReturn(isMemberTagPredicate);
    }

    /**
     * Test the findByNameAndUserAndTags specification.
     */
    @Test
    public void testFindByNameAndUserAndTagsAll() {
        final Specification<Application> spec = ApplicationSpecs
                .findByNameAndUserAndTags(NAME, USER_NAME, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Application_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Application_.user), USER_NAME);
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Application_.tags));
        }
    }

    /**
     * Test the findByNameAndUserAndTags specification.
     */
    @Test
    public void testFindByNameAndUserAndTagsNoName() {
        final Specification<Application> spec = ApplicationSpecs
                .findByNameAndUserAndTags(null, USER_NAME, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(Application_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Application_.user), USER_NAME);
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Application_.tags));
        }
    }

    /**
     * Test the findByNameAndUserAndTags specification.
     */
    @Test
    public void testFindByNameAndUserAndTagsNoUserName() {
        final Specification<Application> spec = ApplicationSpecs
                .findByNameAndUserAndTags(NAME, null, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Application_.name), NAME);
        Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(Application_.user), USER_NAME);
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Application_.tags));
        }
    }

    /**
     * Test the findByNameAndUserAndTags specification.
     */
    @Test
    public void testFindByNameAndUserAndTagsNoTags() {
        final Specification<Application> spec = ApplicationSpecs
                .findByNameAndUserAndTags(NAME, USER_NAME, null);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Application_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Application_.user), USER_NAME);
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.never())
                    .isMember(tag, this.root.get(Application_.tags));
        }
    }

    /**
     * Here for completeness.
     */
    @Test
    public void testProtectedConstructor() {
        final ApplicationSpecs specs = new ApplicationSpecs();
        Assert.assertEquals(ApplicationSpecs.class, specs.getClass());
    }
}
