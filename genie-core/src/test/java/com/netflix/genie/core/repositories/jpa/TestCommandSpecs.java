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
package com.netflix.genie.core.repositories.jpa;

import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Command_;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
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
 * Tests for the command specifications.
 *
 * @author tgianos
 */
public class TestCommandSpecs {

    private static final String NAME = "hive";
    private static final String USER_NAME = "tgianos";
    private static final String TAG_1 = "prod";
    private static final String TAG_2 = "hive";
    private static final String TAG_3 = "11";
    private static final Set<String> TAGS = new HashSet<>();
    private static final Set<CommandStatus> STATUSES = new HashSet<>();

    private Root<Command> root;
    private CriteriaQuery<?> cq;
    private CriteriaBuilder cb;

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
        STATUSES.add(CommandStatus.ACTIVE);
        STATUSES.add(CommandStatus.INACTIVE);

        this.root = (Root<Command>) Mockito.mock(Root.class);
        this.cq = Mockito.mock(CriteriaQuery.class);
        this.cb = Mockito.mock(CriteriaBuilder.class);

        final Path<String> commandNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Command_.name)).thenReturn(commandNamePath);
        Mockito.when(this.cb.equal(commandNamePath, NAME))
                .thenReturn(equalNamePredicate);

        final Path<String> userNamePath = (Path<String>) Mockito.mock(Path.class);
        final Predicate equalUserNamePredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Command_.user)).thenReturn(userNamePath);
        Mockito.when(this.cb.equal(userNamePath, USER_NAME))
                .thenReturn(equalUserNamePredicate);

        final Path<CommandStatus> statusPath = (Path<CommandStatus>) Mockito.mock(Path.class);
        final Predicate equalStatusPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Command_.status)).thenReturn(statusPath);
        Mockito.when(this.cb.equal(Mockito.eq(statusPath), Mockito.any(CommandStatus.class)))
                .thenReturn(equalStatusPredicate);

        final Expression<Set<String>> tagExpression = (Expression<Set<String>>) Mockito.mock(Expression.class);
        final Predicate isMemberTagPredicate = Mockito.mock(Predicate.class);
        Mockito.when(this.root.get(Command_.tags)).thenReturn(tagExpression);
        Mockito.when(this.cb.isMember(Mockito.any(String.class), Mockito.eq(tagExpression)))
                .thenReturn(isMemberTagPredicate);
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindAll() {
        final Specification<Command> spec = CommandSpecs.find(
                NAME, USER_NAME, STATUSES, TAGS
        );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Command_.tags));
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoName() {
        final Specification<Command> spec = CommandSpecs.find(
                null, USER_NAME, STATUSES, TAGS
        );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Command_.tags));
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoUserName() {
        final Specification<Command> spec = CommandSpecs.find(
                NAME, null, STATUSES, TAGS
        );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.never())
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Command_.tags));
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoTags() {
        final Specification<Command> spec = CommandSpecs.find(
                NAME, USER_NAME, STATUSES, null
        );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.never())
                    .isMember(tag, this.root.get(Command_.tags));
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindEmptyTag() {
        TAGS.add("");
        final Specification<Command> spec = CommandSpecs.find(
                NAME, USER_NAME, STATUSES, TAGS
        );

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            if (StringUtils.isBlank(tag)) {
                Mockito.verify(this.cb, Mockito.never())
                        .isMember(tag, this.root.get(Command_.tags));
            } else {
                Mockito.verify(this.cb, Mockito.times(1))
                        .isMember(tag, this.root.get(Command_.tags));
            }
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindNoStatuses() {
        final Specification<Command> spec = CommandSpecs
                .find(NAME, USER_NAME, null, TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never())
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Command_.tags));
        }
    }

    /**
     * Test the find specification.
     */
    @Test
    public void testFindEmptyStatuses() {
        final Specification<Command> spec = CommandSpecs
                .find(NAME, USER_NAME, new HashSet<>(), TAGS);

        spec.toPredicate(this.root, this.cq, this.cb);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.name), NAME);
        Mockito.verify(this.cb, Mockito.times(1))
                .equal(this.root.get(Command_.user), USER_NAME);
        for (final CommandStatus status : STATUSES) {
            Mockito.verify(this.cb, Mockito.never())
                    .equal(this.root.get(Command_.status), status);
        }
        for (final String tag : TAGS) {
            Mockito.verify(this.cb, Mockito.times(1))
                    .isMember(tag, this.root.get(Command_.tags));
        }
    }

    /**
     * Here for completeness.
     */
    @Test
    public void testProtectedConstructor() {
        Assert.assertNotNull(new CommandSpecs());
    }
}
