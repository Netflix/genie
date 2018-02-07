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
package com.netflix.genie.web.jpa.specifications;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity_;
import com.netflix.genie.web.jpa.entities.TagEntity;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.SetJoin;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * Tests for the application specifications.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class JpaClusterSpecsUnitTests {

    private static final String NAME = "h2prod";
    private static final TagEntity TAG_1 = new TagEntity("prod");
    private static final TagEntity TAG_2 = new TagEntity("yarn");
    private static final TagEntity TAG_3 = new TagEntity("hadoop");
    private static final ClusterStatus STATUS_1 = ClusterStatus.UP;
    private static final ClusterStatus STATUS_2 = ClusterStatus.OUT_OF_SERVICE;
    private static final Set<TagEntity> TAGS = Sets.newHashSet(TAG_1, TAG_2, TAG_3);
    private static final Set<ClusterStatus> STATUSES = EnumSet.noneOf(ClusterStatus.class);
    private static final Instant MIN_UPDATE_TIME = Instant.ofEpochMilli(123467L);
    private static final Instant MAX_UPDATE_TIME = Instant.ofEpochMilli(1234643L);

    private Root<ClusterEntity> root;
    private CriteriaQuery<?> cq;
    private CriteriaBuilder cb;
    private SetJoin<ClusterEntity, TagEntity> tagEntityJoin;

    /**
     * Setup test wide variables.
     */
    @BeforeClass
    public static void setupClass() {
        STATUSES.add(STATUS_1);
        STATUSES.add(STATUS_2);
    }

    /**
     * Setup some variables.
     */
    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        this.root = (Root<ClusterEntity>) Mockito.mock(Root.class);
        this.cq = Mockito.mock(CriteriaQuery.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);

        final Path<Long> idPath = (Path<Long>) Mockito.mock(Path.class);
        Mockito.when(this.root.get(ClusterEntity_.id)).thenReturn(idPath);

        final Path<String> clusterNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeNamePredicate = Mockito.mock(Predicate.class);
        final Predicate equalNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.name)).thenReturn(clusterNamePath);
        Mockito.when(this.cb.like(clusterNamePath, NAME)).thenReturn(likeNamePredicate);
        Mockito.when(this.cb.equal(clusterNamePath, NAME)).thenReturn(equalNamePredicate);

        final Path<Instant> minUpdatePath = (Path<Instant>) Mockito.mock(Path.class);
        final Predicate greaterThanOrEqualToPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.updated)).thenReturn(minUpdatePath);
        Mockito.when(this.cb.greaterThanOrEqualTo(minUpdatePath, MIN_UPDATE_TIME))
            .thenReturn(greaterThanOrEqualToPredicate);

        final Path<Instant> maxUpdatePath = (Path<Instant>) Mockito.mock(Path.class);
        final Predicate lessThanPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.updated)).thenReturn(maxUpdatePath);
        Mockito.when(this.cb.lessThan(maxUpdatePath, MAX_UPDATE_TIME)).thenReturn(lessThanPredicate);

        final Path<ClusterStatus> statusPath = (Path<ClusterStatus>) Mockito.mock(Path.class);
        final Predicate equalStatusPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.status)).thenReturn(statusPath);
        Mockito.when(this.cb.equal(Mockito.eq(statusPath), Mockito.any(ClusterStatus.class)))
            .thenReturn(equalStatusPredicate);

        this.tagEntityJoin = (SetJoin<ClusterEntity, TagEntity>) Mockito.mock(SetJoin.class);
        Mockito.when(this.root.join(ClusterEntity_.tags)).thenReturn(this.tagEntityJoin);
        final Predicate tagInPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.tagEntityJoin.in(TAGS)).thenReturn(tagInPredicate);

        final Expression<Long> idCountExpression = (Expression<Long>) Mockito.mock(Expression.class);
        Mockito.when(this.cb.count(Mockito.any())).thenReturn(idCountExpression);
        final Predicate havingPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.cb.equal(idCountExpression, TAGS.size())).thenReturn(havingPredicate);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindAll() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                NAME,
                STATUSES,
                TAGS,
                MIN_UPDATE_TIME,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindAllLike() {
        final String newName = NAME + "%";
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                newName,
                STATUSES,
                TAGS,
                MIN_UPDATE_TIME,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.name), newName);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoName() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                null,
                STATUSES,
                TAGS,
                MIN_UPDATE_TIME,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.never())
            .like(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.never())
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoStatuses() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                NAME,
                null,
                TAGS,
                MIN_UPDATE_TIME,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(
            this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindEmptyStatuses() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                NAME,
                EnumSet.noneOf(ClusterStatus.class),
                TAGS,
                MIN_UPDATE_TIME,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1))
            .lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoTags() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                NAME,
                STATUSES,
                null,
                MIN_UPDATE_TIME,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1))
            .lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.never()).join(ClusterEntity_.tags);
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoMinTime() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                NAME,
                STATUSES,
                TAGS,
                null,
                MAX_UPDATE_TIME
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.never())
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.times(1))
            .lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoMax() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs
            .find(
                NAME,
                STATUSES,
                TAGS,
                MIN_UPDATE_TIME,
                null
            );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(this.root.get(ClusterEntity_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(ClusterEntity_.updated), MIN_UPDATE_TIME);
        Mockito.verify(this.cb, Mockito.never())
            .lessThan(this.root.get(ClusterEntity_.updated), MAX_UPDATE_TIME);
        Mockito.verify(this.root, Mockito.times(1)).join(ClusterEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }
}
