/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.data.entities;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests for the AgentConnectionEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
class AgentConnectionEntityTest extends EntityTestBase {

    private static final String JOB = UUID.randomUUID().toString();
    private static final String HOST = UUID.randomUUID().toString();

    /**
     * Test constructor.
     */
    @Test
    void canCreateAgentConnectionEntityWithConstructor() {
        final AgentConnectionEntity entity = new AgentConnectionEntity(JOB, HOST);
        Assertions.assertThat(entity.getJobId()).isEqualTo(JOB);
        Assertions.assertThat(entity.getServerHostname()).isEqualTo(HOST);
    }

    /**
     * Test setters.
     */
    @Test
    void canCreateAgentConnectionEntityWithSetters() {
        final AgentConnectionEntity entity = new AgentConnectionEntity();
        entity.setJobId(JOB);
        entity.setServerHostname(HOST);
        Assertions.assertThat(entity.getJobId()).isEqualTo(JOB);
        Assertions.assertThat(entity.getServerHostname()).isEqualTo(HOST);
    }

    /**
     * Verify validated fields value.
     */
    @Test
    void cantCreateAgentConnectionEntityDueToSize() {

        final List<Pair<String, String>> invalidParameterPairs = Lists.newArrayList();

        invalidParameterPairs.add(Pair.of(JOB, null));
        invalidParameterPairs.add(Pair.of(null, HOST));
        invalidParameterPairs.add(Pair.of(JOB, " "));
        invalidParameterPairs.add(Pair.of(" ", HOST));
        invalidParameterPairs.add(Pair.of(StringUtils.rightPad(JOB, 256), HOST));
        invalidParameterPairs.add(Pair.of(JOB, StringUtils.rightPad(HOST, 256)));

        for (final Pair<String, String> invalidParameters : invalidParameterPairs) {
            final AgentConnectionEntity entity =
                new AgentConnectionEntity(invalidParameters.getLeft(), invalidParameters.getRight());

            try {
                this.validate(entity);
            } catch (final ConstraintViolationException e) {
                // Expected, move on to the next pair.
                continue;
            }

            Assertions.fail("Entity unexpectedly passed validation: " + entity.toString());
        }
    }

    /**
     * Test to make sure equals and hash code only care about the unique file.
     */
    @Test
    void testEqualsAndHashCode() {
        final AgentConnectionEntity e1 = new AgentConnectionEntity(JOB, HOST);
        final AgentConnectionEntity e2 = new AgentConnectionEntity();
        e2.setJobId(JOB);
        e2.setServerHostname(HOST);

        Assertions.assertThat(e1).isEqualTo(e2);
        Assertions.assertThat(e1.hashCode()).isEqualTo(e2.hashCode());

        final AgentConnectionEntity e3 = new AgentConnectionEntity(JOB, "FOO");
        final AgentConnectionEntity e4 = new AgentConnectionEntity("BAR", HOST);

        Assertions.assertThat(e1).isNotEqualTo(e3);
        Assertions.assertThat(e2).isNotEqualTo(e3);
        Assertions.assertThat(e1).isNotEqualTo(e4);
        Assertions.assertThat(e2).isNotEqualTo(e4);
        Assertions.assertThat(e3).isNotEqualTo(e4);

        Assertions.assertThat(e1.hashCode()).isNotEqualTo(e3.hashCode());
        Assertions.assertThat(e2.hashCode()).isNotEqualTo(e3.hashCode());
        Assertions.assertThat(e1.hashCode()).isNotEqualTo(e4.hashCode());
        Assertions.assertThat(e2.hashCode()).isNotEqualTo(e4.hashCode());
        Assertions.assertThat(e3.hashCode()).isNotEqualTo(e4.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(new AgentConnectionEntity().toString()).isNotBlank();
    }
}
