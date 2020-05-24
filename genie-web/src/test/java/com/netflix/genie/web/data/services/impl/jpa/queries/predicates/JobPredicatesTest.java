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
package com.netflix.genie.web.data.services.impl.jpa.queries.predicates;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity_;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for {@link JobPredicates}.
 *
 * @author tgianos
 */
class JobPredicatesTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String JOB_NAME = "jobName";
    private static final String USER_NAME = "tgianos";
    private static final String CLUSTER_NAME = "hprod2";
    private static final ClusterEntity CLUSTER = Mockito.mock(ClusterEntity.class);
    private static final String COMMAND_NAME = "pig";
    private static final CommandEntity COMMAND = Mockito.mock(CommandEntity.class);
    private static final Set<String> TAGS = Sets.newHashSet();
    private static final Set<String> STATUSES = Sets.newHashSet();
    private static final String TAG = UUID.randomUUID().toString();
    private static final Instant MIN_STARTED = Instant.now();
    private static final Instant MAX_STARTED = MIN_STARTED.plus(10, ChronoUnit.MILLIS);
    private static final Instant MIN_FINISHED = MAX_STARTED.plus(10, ChronoUnit.MILLIS);
    private static final Instant MAX_FINISHED = MIN_FINISHED.plus(10, ChronoUnit.MILLIS);
    private static final String GROUPING = UUID.randomUUID().toString();
    private static final String GROUPING_INSTANCE = UUID.randomUUID().toString();

    private Root<JobEntity> root;
    private CriteriaBuilder cb;
    private String tagLikeStatement;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        TAGS.clear();
        TAGS.add(TAG);

        STATUSES.clear();
        STATUSES.add(JobStatus.INIT.name());
        STATUSES.add(JobStatus.FAILED.name());

        this.root = (Root<JobEntity>) Mockito.mock(Root.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);

        final Path<String> idPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeIdPredicate = Mockito.mock(Predicate.class);
        final Predicate equalIdPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.uniqueId)).thenReturn(idPath);
        Mockito.when(this.cb.like(idPath, ID)).thenReturn(likeIdPredicate);
        Mockito.when(this.cb.equal(idPath, ID)).thenReturn(equalIdPredicate);

        final Path<String> jobNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeJobNamePredicate = Mockito.mock(Predicate.class);
        final Predicate equalJobNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.name)).thenReturn(jobNamePath);
        Mockito.when(this.cb.like(jobNamePath, JOB_NAME)).thenReturn(likeJobNamePredicate);
        Mockito.when(this.cb.equal(jobNamePath, JOB_NAME)).thenReturn(equalJobNamePredicate);

        final Path<String> userNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalUserNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.user)).thenReturn(userNamePath);
        Mockito.when(this.cb.equal(userNamePath, USER_NAME)).thenReturn(equalUserNamePredicate);

        final Path<String> statusPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalStatusPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.status)).thenReturn(statusPath);
        Mockito
            .when(this.cb.equal(Mockito.eq(statusPath), Mockito.any(JobStatus.class)))
            .thenReturn(equalStatusPredicate);

        final Path<String> clusterNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalClusterNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.clusterName)).thenReturn(clusterNamePath);
        Mockito.when(this.cb.equal(clusterNamePath, CLUSTER_NAME)).thenReturn(equalClusterNamePredicate);

        final Path<ClusterEntity> clusterIdPath = (Path<ClusterEntity>) Mockito.mock(Path.class);
        final Predicate equalClusterIdPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.cluster)).thenReturn(clusterIdPath);
        Mockito.when(this.cb.equal(clusterIdPath, CLUSTER)).thenReturn(equalClusterIdPredicate);

        final Path<String> commandNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalCommandNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.commandName)).thenReturn(commandNamePath);
        Mockito.when(this.cb.equal(commandNamePath, COMMAND_NAME)).thenReturn(equalCommandNamePredicate);

        final Path<CommandEntity> commandIdPath = (Path<CommandEntity>) Mockito.mock(Path.class);
        final Predicate equalCommandIdPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.command)).thenReturn(commandIdPath);
        Mockito.when(this.cb.equal(clusterIdPath, COMMAND)).thenReturn(equalCommandIdPredicate);

        final Path<String> tagPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate likeTagPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.tagSearchString)).thenReturn(tagPath);
        Mockito.when(this.cb.like(Mockito.eq(tagPath), Mockito.any(String.class))).thenReturn(likeTagPredicate);

        this.tagLikeStatement = PredicateUtils.getTagLikeString(TAGS);

        final Path<Instant> startedPath = (Path<Instant>) Mockito.mock(Path.class);
        final Predicate minStartedPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.started)).thenReturn(startedPath);
        Mockito
            .when(this.cb.greaterThanOrEqualTo(Mockito.eq(startedPath), Mockito.eq(MIN_STARTED)))
            .thenReturn(minStartedPredicate);

        final Predicate maxStartedPredicate = Mockito.mock(Predicate.class);
        Mockito
            .when(this.cb.lessThan(Mockito.eq(startedPath), Mockito.eq(MAX_STARTED)))
            .thenReturn(maxStartedPredicate);

        final Path<Instant> finishedPath = (Path<Instant>) Mockito.mock(Path.class);
        final Predicate minFinishedPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.finished)).thenReturn(finishedPath);
        Mockito
            .when(this.cb.greaterThanOrEqualTo(Mockito.eq(finishedPath), Mockito.eq(MIN_FINISHED)))
            .thenReturn(minFinishedPredicate);

        final Predicate maxFinishedPredicate = Mockito.mock(Predicate.class);
        Mockito
            .when(this.cb.lessThan(Mockito.eq(finishedPath), Mockito.eq(MAX_FINISHED)))
            .thenReturn(maxFinishedPredicate);

        final Path<String> groupingPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalGroupingPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.grouping)).thenReturn(groupingPath);
        Mockito.when(this.cb.equal(groupingPath, GROUPING)).thenReturn(equalGroupingPredicate);

        final Path<String> groupingInstancePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalGroupingInstancePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(JobEntity_.groupingInstance)).thenReturn(groupingInstancePath);
        Mockito.when(this.cb.equal(groupingInstancePath, GROUPING_INSTANCE)).thenReturn(equalGroupingInstancePredicate);
    }

    @Test
    void testFindWithAll() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito.
            verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithAllLikes() {
        final String newId = ID + "%";
        final String newName = JOB_NAME + "%";
        final String newUserName = USER_NAME + "%";
        final String newClusterName = CLUSTER_NAME + "%";
        final String newCommandName = COMMAND_NAME + "%";
        final String newGrouping = GROUPING + "%";
        final String newGroupingInstance = GROUPING_INSTANCE + "%";
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            newId,
            newName,
            newUserName,
            STATUSES,
            TAGS,
            newClusterName,
            CLUSTER,
            newCommandName,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            newGrouping,
            newGroupingInstance
        );

        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.uniqueId), newId);
        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.name), newName);
        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.user), newUserName);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.clusterName), newClusterName);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.commandName), newCommandName);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.grouping), newGrouping);
        Mockito.verify(this.cb, Mockito.times(1)).like(this.root.get(JobEntity_.groupingInstance), newGroupingInstance);
    }

    @Test
    void testFindWithOutId() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            null,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.never()).like(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutJobName() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            null,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.never()).like(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutUserName() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            null,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.user), USER_NAME);
        Mockito.verify(this.cb, Mockito.never()).like(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutStatus() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            null,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithEmptyStatus() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            Sets.newHashSet(),
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutClusterName() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            null,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.never()).like(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutClusterId() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            null,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutCommandName() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            null,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.never()).like(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutCommandId() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            null,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutTags() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            null,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito.verify(this.cb, Mockito.never()).like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutMinStarted() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            null,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.never()).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutMaxStarted() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            null,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.never()).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutMinFinished() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            null,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.never())
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutMaxFinished() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            null,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.never()).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithEmptyTag() {
        TAGS.add("");
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
    }

    @Test
    void testFindWithOutGrouping() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            null,
            GROUPING_INSTANCE
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }

    @Test
    void testFindWithOutGroupingInstance() {
        JobPredicates.getFindPredicate(
            this.root,
            this.cb,
            ID,
            JOB_NAME,
            USER_NAME,
            STATUSES,
            TAGS,
            CLUSTER_NAME,
            CLUSTER,
            COMMAND_NAME,
            COMMAND,
            MIN_STARTED,
            MAX_STARTED,
            MIN_FINISHED,
            MAX_FINISHED,
            GROUPING,
            null
        );

        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.uniqueId), ID);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.name), JOB_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.status), status);
        }
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.clusterName), CLUSTER_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.cluster), CLUSTER);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.commandName), COMMAND_NAME);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.command), COMMAND);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(JobEntity_.tagSearchString), this.tagLikeStatement);
        Mockito.verify(this.cb, Mockito.times(1)).greaterThanOrEqualTo(this.root.get(JobEntity_.started), MIN_STARTED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.started), MAX_STARTED);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .greaterThanOrEqualTo(this.root.get(JobEntity_.finished), MIN_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).lessThan(this.root.get(JobEntity_.finished), MAX_FINISHED);
        Mockito.verify(this.cb, Mockito.times(1)).equal(this.root.get(JobEntity_.grouping), GROUPING);
        Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(JobEntity_.groupingInstance), GROUPING_INSTANCE);
    }
}
