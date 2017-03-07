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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity_;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity_;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.ListJoin;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for the application specifications.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class JpaClusterSpecsUnitTests {

    private static final String NAME = "h2prod";
    private static final String TAG_1 = "prod";
    private static final String TAG_2 = "yarn";
    private static final String TAG_3 = "hadoop";
    private static final String COMMAND_CRITERIA_TAG_1 = "hive";
    private static final String COMMAND_CRITERIA_TAG_2 = "prod";
    private static final ClusterStatus STATUS_1 = ClusterStatus.UP;
    private static final ClusterStatus STATUS_2 = ClusterStatus.OUT_OF_SERVICE;
    private static final Set<String> TAGS = new HashSet<>();
    private static final Set<ClusterStatus> STATUSES = EnumSet.noneOf(ClusterStatus.class);
    private static final Date MIN_UPDATE_TIME = new Date(123467L);
    private static final Date MAX_UPDATE_TIME = new Date(1234643L);
    private static final Set<String> CLUSTER_CRITERIA_TAGS = new HashSet<>();
    private static final Set<String> COMMAND_CRITERIA = new HashSet<>();

    private Root<ClusterEntity> root;
    private CriteriaQuery<?> cq;
    private CriteriaBuilder cb;
    private ListJoin<ClusterEntity, CommandEntity> commands;
    private String tagLikeStatement;
    private String commandLikeStatement;

    /**
     * Setup test wide variables.
     */
    @BeforeClass
    public static void setupClass() {
        STATUSES.add(STATUS_1);
        STATUSES.add(STATUS_2);

        CLUSTER_CRITERIA_TAGS.add(TAG_1);
        CLUSTER_CRITERIA_TAGS.add(TAG_2);
        CLUSTER_CRITERIA_TAGS.add(TAG_3);

        COMMAND_CRITERIA.add(COMMAND_CRITERIA_TAG_1);
        COMMAND_CRITERIA.add(COMMAND_CRITERIA_TAG_2);
    }

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

        this.root = (Root<ClusterEntity>) Mockito.mock(Root.class);
        this.cq = Mockito.mock(CriteriaQuery.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);
        this.commands = (ListJoin<ClusterEntity, CommandEntity>) Mockito.mock(ListJoin.class);

        final Path<String> clusterNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeNamePredicate = Mockito.mock(Predicate.class);
        final Predicate equalNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.name)).thenReturn(clusterNamePath);
        Mockito.when(this.cb.like(clusterNamePath, NAME)).thenReturn(likeNamePredicate);
        Mockito.when(this.cb.equal(clusterNamePath, NAME)).thenReturn(equalNamePredicate);

        final Path<Date> minUpdatePath = (Path<Date>) Mockito.mock(Path.class);
        final Predicate greaterThanOrEqualToPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.updated)).thenReturn(minUpdatePath);
        Mockito.when(this.cb.greaterThanOrEqualTo(minUpdatePath, MIN_UPDATE_TIME))
            .thenReturn(greaterThanOrEqualToPredicate);

        final Path<Date> maxUpdatePath = (Path<Date>) Mockito.mock(Path.class);
        final Predicate lessThanPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.updated)).thenReturn(maxUpdatePath);
        Mockito.when(this.cb.lessThan(maxUpdatePath, MAX_UPDATE_TIME)).thenReturn(lessThanPredicate);

        final Path<ClusterStatus> statusPath = (Path<ClusterStatus>) Mockito.mock(Path.class);
        final Predicate equalStatusPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.status)).thenReturn(statusPath);
        Mockito.when(this.cb.equal(Mockito.eq(statusPath), Mockito.any(ClusterStatus.class)))
            .thenReturn(equalStatusPredicate);

        final Path<String> tagPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeTagPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(ClusterEntity_.tags)).thenReturn(tagPath);
        Mockito.when(this.cb.like(Mockito.eq(tagPath), Mockito.any(String.class))).thenReturn(likeTagPredicate);

        this.tagLikeStatement = JpaSpecificationUtils.getTagLikeString(TAGS);

        this.commandLikeStatement = JpaSpecificationUtils.getTagLikeString(
            Sets.newHashSet(COMMAND_CRITERIA_TAG_1, COMMAND_CRITERIA_TAG_2)
        );

        // Setup for findByClusterAndCommandCriteria
        Mockito.when(this.root.join(ClusterEntity_.commands)).thenReturn(this.commands);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.never())
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindEmptyTag() {
        TAGS.add("");
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
        Mockito.verify(this.cb, Mockito.times(1))
            .like(this.root.get(ClusterEntity_.tags), this.tagLikeStatement);
        for (final ClusterStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(ClusterEntity_.status), status);
        }
    }

    /**
     * Test to make sure no member of predicates are added.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFindByClusterAndCommandCriteriaNoCriteria() {
        final Specification<ClusterEntity> spec = JpaClusterSpecs.findByClusterAndCommandCriteria(null, null);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cq, Mockito.times(1)).distinct(true);
        Mockito.verify(this.commands, Mockito.times(1)).get(CommandEntity_.status);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.commands.get(CommandEntity_.status), CommandStatus.ACTIVE);
        Mockito.verify(this.root, Mockito.times(1)).get(ClusterEntity_.status);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(ClusterEntity_.status), ClusterStatus.UP);
        Mockito.verify(this.cb, Mockito.never()).isMember(Mockito.any(String.class), Mockito.any(Expression.class));
    }

    /**
     * Test to all predicates are added.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testFindByClusterAndCommandCriteria() throws GenieException {
        final ClusterCriteria criteria = new ClusterCriteria(CLUSTER_CRITERIA_TAGS);
        final Specification<ClusterEntity> spec
            = JpaClusterSpecs.findByClusterAndCommandCriteria(criteria, COMMAND_CRITERIA);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cq, Mockito.times(1)).distinct(true);
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(Mockito.eq(this.commands.get(CommandEntity_.status)), Mockito.eq(CommandStatus.ACTIVE));
        Mockito.verify(this.cb, Mockito.times(1))
            .equal(Mockito.eq(this.root.get(ClusterEntity_.status)), Mockito.eq(ClusterStatus.UP));
        Mockito.verify(this.cb, Mockito.times(1))
            .like(Mockito.eq(this.root.get(ClusterEntity_.tags)), Mockito.eq(this.tagLikeStatement));
        Mockito.verify(this.cb, Mockito.times(1))
            .like(Mockito.eq(this.commands.get(CommandEntity_.tags)), Mockito.eq(this.commandLikeStatement));
    }

    /**
     * Here for completeness.
     */
    @Test
    public void testProtectedConstructor() {
        Assert.assertNotNull(new JpaClusterSpecs());
    }
}
