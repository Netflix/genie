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
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.SetJoin;
import java.util.Set;

/**
 * Tests for {@link CommandPredicates}.
 *
 * @author tgianos
 */
class CommandPredicatesTest {

    private static final String NAME = "hive";
    private static final String USER_NAME = "tgianos";
    private static final TagEntity TAG_1 = new TagEntity("prod");
    private static final TagEntity TAG_2 = new TagEntity("hive");
    private static final TagEntity TAG_3 = new TagEntity("11");
    private static final Set<TagEntity> TAGS = Sets.newHashSet(TAG_1, TAG_2, TAG_3);
    private static final Set<String> STATUSES = Sets.newHashSet(
        CommandStatus.ACTIVE.name(),
        CommandStatus.INACTIVE.name()
    );

    private Root<CommandEntity> root;
    private CriteriaQuery<?> cq;
    private CriteriaBuilder cb;
    private SetJoin<CommandEntity, TagEntity> tagEntityJoin;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        this.root = (Root<CommandEntity>) Mockito.mock(Root.class);
        this.cq = Mockito.mock(CriteriaQuery.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);

        final Path<Long> idPath = (Path<Long>) Mockito.mock(Path.class);
        Mockito.when(this.root.get(CommandEntity_.id)).thenReturn(idPath);

        final Path<String> commandNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalNamePredicate = Mockito.mock(Predicate.class);
        final Predicate likeNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(CommandEntity_.name)).thenReturn(commandNamePath);
        Mockito.when(this.cb.equal(commandNamePath, NAME)).thenReturn(equalNamePredicate);
        Mockito.when(this.cb.like(commandNamePath, NAME)).thenReturn(likeNamePredicate);

        final Path<String> userNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalUserNamePredicate = Mockito.mock(Predicate.class);
        final Predicate likeUserNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(CommandEntity_.user)).thenReturn(userNamePath);
        Mockito.when(this.cb.equal(userNamePath, USER_NAME)).thenReturn(equalUserNamePredicate);
        Mockito.when(this.cb.like(userNamePath, USER_NAME)).thenReturn(likeUserNamePredicate);

        final Path<String> statusPath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalStatusPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(CommandEntity_.status)).thenReturn(statusPath);
        Mockito
            .when(this.cb.equal(Mockito.eq(statusPath), Mockito.anyString()))
            .thenReturn(equalStatusPredicate);

        this.tagEntityJoin = (SetJoin<CommandEntity, TagEntity>) Mockito.mock(SetJoin.class);
        Mockito.when(this.root.join(CommandEntity_.tags)).thenReturn(this.tagEntityJoin);
        final Predicate tagInPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.tagEntityJoin.in(TAGS)).thenReturn(tagInPredicate);

        final Expression<Long> idCountExpression = (Expression<Long>) Mockito.mock(Expression.class);
        Mockito.when(this.cb.count(Mockito.any())).thenReturn(idCountExpression);
        final Predicate havingPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.cb.equal(idCountExpression, TAGS.size())).thenReturn(havingPredicate);
    }

    @Test
    void testFindAll() {
        CommandPredicates.find(this.root, this.cq, this.cb, NAME, USER_NAME, STATUSES, TAGS);

        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.name), NAME);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito
                .verify(this.cb, Mockito.times(1))
                .equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.times(1)).join(CommandEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
    }

    @Test
    void testFindAllLike() {
        final String newName = NAME + "%";
        final String newUser = USER_NAME + "%";
        CommandPredicates.find(this.root, this.cq, this.cb, newName, newUser, STATUSES, TAGS);

        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(CommandEntity_.name), newName);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .like(this.root.get(CommandEntity_.user), newUser);
        for (final String status : STATUSES) {
            Mockito
                .verify(this.cb, Mockito.times(1))
                .equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.times(1)).join(CommandEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
    }

    @Test
    void testFindNoName() {
        CommandPredicates.find(this.root, this.cq, this.cb, null, USER_NAME, STATUSES, TAGS);

        Mockito
            .verify(this.cb, Mockito.never())
            .equal(this.root.get(CommandEntity_.name), NAME);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito
                .verify(this.cb, Mockito.times(1))
                .equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.times(1)).join(CommandEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
    }

    @Test
    void testFindNoUserName() {
        CommandPredicates.find(this.root, this.cq, this.cb, NAME, null, STATUSES, TAGS);

        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.name), NAME);
        Mockito
            .verify(this.cb, Mockito.never())
            .equal(this.root.get(CommandEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito
                .verify(this.cb, Mockito.times(1))
                .equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.times(1)).join(CommandEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
    }

    @Test
    void testFindNoTags() {
        CommandPredicates.find(this.root, this.cq, this.cb, NAME, USER_NAME, STATUSES, null);

        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.name), NAME);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito
                .verify(this.cb, Mockito.times(1))
                .equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.never()).join(CommandEntity_.tags);
    }

    @Test
    void testFindNoStatuses() {
        CommandPredicates.find(this.root, this.cq, this.cb, NAME, USER_NAME, null, TAGS);

        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.name), NAME);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito
                .verify(this.cb, Mockito.never())
                .equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.times(1)).join(CommandEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
    }

    @Test
    void testFindEmptyStatuses() {
        CommandPredicates.find(this.root, this.cq, this.cb, NAME, USER_NAME, Sets.newHashSet(), TAGS);

        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.name), NAME);
        Mockito
            .verify(this.cb, Mockito.times(1))
            .equal(this.root.get(CommandEntity_.user), USER_NAME);
        for (final String status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never()).equal(this.root.get(CommandEntity_.status), status);
        }
        Mockito.verify(this.root, Mockito.times(1)).join(CommandEntity_.tags);
        Mockito.verify(this.tagEntityJoin, Mockito.times(1)).in(TAGS);
        Mockito.verify(this.cq, Mockito.times(1)).groupBy(Mockito.any(Path.class));
        Mockito.verify(this.cq, Mockito.times(1)).having(Mockito.any(Predicate.class));
    }
}
