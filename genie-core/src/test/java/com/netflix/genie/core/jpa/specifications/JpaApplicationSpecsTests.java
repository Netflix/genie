/*
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jpa.specifications;

import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ApplicationEntity_;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
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
public class JpaApplicationSpecsTests {

    private static final String NAME = "tez";
    private static final String USER_NAME = "tgianos";
    private static final String TAG_1 = "tez";
    private static final String TAG_2 = "yarn";
    private static final String TAG_3 = "hadoop";
    private static final Set<String> TAGS = new HashSet<>();
    private static final Set<ApplicationStatus> STATUSES = new HashSet<>();

    private Root<ApplicationEntity> root;
    private CriteriaQuery<?> cq;
    private CriteriaBuilder cb;
    private String tagLikeStatement;

    /**
     * Setup some variables.
     */
    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        TAGS.clear();
        TAGS.add(TAG_1);
        TAGS.add(TAG_2);
        TAGS.add(TAG_3);

        STATUSES.clear();
        STATUSES.add(ApplicationStatus.ACTIVE);
        STATUSES.add(ApplicationStatus.DEPRECATED);

        this.root = (Root<ApplicationEntity>) Mockito.mock(Root.class);
        this.cq = Mockito.mock(CriteriaQuery.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);

        final Path<String> commandNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ApplicationEntity_.name)).thenReturn(commandNamePath);
        Mockito.when(this.cb.equal(commandNamePath, NAME))
                .thenReturn(equalNamePredicate);

        final Path<String> userNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalUserNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ApplicationEntity_.user)).thenReturn(userNamePath);
        Mockito.when(this.cb.equal(userNamePath, USER_NAME))
                .thenReturn(equalUserNamePredicate);

        final Path<ApplicationStatus> statusPath = (Path<ApplicationStatus>) Mockito.mock(Path.class);
        final Predicate equalStatusPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ApplicationEntity_.status)).thenReturn(statusPath);
        Mockito.when(this.cb.equal(Mockito.eq(statusPath), Mockito.any(ApplicationStatus.class)))
                .thenReturn(equalStatusPredicate);

        final Path<String> tagPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeTagPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ApplicationEntity_.sortedTags)).thenReturn(tagPath);
        Mockito.when(this.cb.like(Mockito.eq(tagPath), Mockito.any(String.class)))
                .thenReturn(likeTagPredicate);

        this.tagLikeStatement = JpaSpecificationUtils.getTagLikeString(TAGS);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindAll() {
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(NAME, USER_NAME, STATUSES, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1))
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoName() {
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(null, USER_NAME, STATUSES, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1))
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoUserName() {
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(NAME, null, STATUSES, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1))
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoStatuses() {
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(NAME, USER_NAME, null, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never())
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1))
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindEmptyStatuses() {
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(NAME, USER_NAME, new HashSet<>(), TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never())
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1))
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoTags() {
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(NAME, USER_NAME, STATUSES, null);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.never())
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindEmptyTag() {
        TAGS.add("");
        final Specification<ApplicationEntity> spec = JpaApplicationSpecs
                .find(NAME, USER_NAME, STATUSES, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ApplicationEntity_.user), USER_NAME);
        for (final ApplicationStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(ApplicationEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1))
                .like(this.root.get(ApplicationEntity_.sortedTags), this.tagLikeStatement);
    }

    /**
     * Here for completeness.
     */
    @Test
    public void testProtectedConstructor() {
        Assert.assertNotNull(new JpaApplicationSpecs());
    }
}
