/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.security.oauth2.pingfederate;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ConfigurationCondition;

import java.util.UUID;

/**
 * Tests for the Ping Federate Security Conditions.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class PingFederateConditionsUnitTests {

    /**
     * Test the default constructor.
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(new PingFederateSecurityConditions());
    }

    /**
     * Test the PingFederateRemoteEnabled class.
     */
    @Test
    public void testPingFederateRemoteEnabledConfiguration() {
        final PingFederateSecurityConditions.PingFederateRemoteEnabled remoteEnabled
            = new PingFederateSecurityConditions.PingFederateRemoteEnabled();
        Assert.assertThat(
            remoteEnabled.getConfigurationPhase(),
            Matchers.is(ConfigurationCondition.ConfigurationPhase.PARSE_CONFIGURATION)
        );
    }

    /**
     * Test to make sure that when no supported security is enabled the class doesn't fire.
     *
     * @throws Exception on any error
     */
    @Test
    public void cantEnableRemoteBeanWithoutAnySecurityEnabled() throws Exception {
        final AnnotationConfigApplicationContext context
            = this.load(PingFederateRemoteEnabledConfig.class);
        Assert.assertFalse(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is enabled the class fires.
     *
     * @throws Exception on any error
     */
    @Test
    public void canEnableBeanWithAllRemoteConditionsEnabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            PingFederateRemoteEnabledConfig.class,
            "genie.security.oauth2.enabled:true",
            "genie.security.oauth2.pingfederate.enabled:true",
            "genie.security.oauth2.pingfederate.jwt.enabled:false"
        );
        Assert.assertTrue(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is missing the class doesn't fire.
     *
     * @throws Exception on any error
     */
    @Test
    public void cantEnableBeanWithAnyRemoteConditionsMissing() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            PingFederateRemoteEnabledConfig.class,
            "genie.security.oauth2.enabled:true",
            "genie.security.oauth2.pingfederate.enabled:false",
            "genie.security.oauth2.pingfederate.jwt.enabled:false"
        );
        Assert.assertFalse(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test the PingFederateJWTEnabled class.
     */
    @Test
    public void testPingFederateJWTEnabledConfiguration() {
        final PingFederateSecurityConditions.PingFederateJWTEnabled jwtEnabled
            = new PingFederateSecurityConditions.PingFederateJWTEnabled();
        Assert.assertThat(
            jwtEnabled.getConfigurationPhase(),
            Matchers.is(ConfigurationCondition.ConfigurationPhase.PARSE_CONFIGURATION)
        );
    }

    /**
     * Test to make sure that when no supported security is enabled the class doesn't fire.
     *
     * @throws Exception on any error
     */
    @Test
    public void cantEnableJWTBeanWithoutAnySecurityEnabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(PingFederateJWTEnabledConfig.class);
        Assert.assertFalse(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is enabled the class fires.
     *
     * @throws Exception on any error
     */
    @Test
    public void canEnableBeanWithAllJWTConditionsEnabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            PingFederateJWTEnabledConfig.class,
            "genie.security.oauth2.enabled:true",
            "genie.security.oauth2.pingfederate.enabled:true",
            "genie.security.oauth2.pingfederate.jwt.enabled:true"
        );
        Assert.assertTrue(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is missing the class doesn't fire.
     *
     * @throws Exception on any error
     */
    @Test
    public void cantEnableBeanWithAnyJWTConditionsMissing() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            PingFederateJWTEnabledConfig.class,
            "genie.security.oauth2.enabled:true",
            "genie.security.oauth2.pingfederate.enabled:true",
            "genie.security.oauth2.pingfederate.jwt.enabled:false"
        );
        Assert.assertFalse(context.containsBean("myBean"));
        context.close();
    }

    private AnnotationConfigApplicationContext load(final Class<?> config, final String... env) {
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        final TestPropertyValues propertyValues = TestPropertyValues.of(env);
        propertyValues.applyTo(context);
        context.register(config);
        context.refresh();
        return context;
    }

    /**
     * Configuration class for testing PingFederateRemoteEnabled.
     */
    @Configuration
    @Conditional(PingFederateSecurityConditions.PingFederateRemoteEnabled.class)
    public static class PingFederateRemoteEnabledConfig {

        /**
         * Stupid placeholder for tests.
         *
         * @return The bean
         */
        @Bean
        public String myBean() {
            return UUID.randomUUID().toString();
        }
    }

    /**
     * Configuration class for testing AnySecurityEnabled.
     */
    @Configuration
    @Conditional(PingFederateSecurityConditions.PingFederateJWTEnabled.class)
    public static class PingFederateJWTEnabledConfig {

        /**
         * Stupid placeholder for tests.
         *
         * @return The bean
         */
        @Bean
        public String myBean() {
            return UUID.randomUUID().toString();
        }
    }
}
