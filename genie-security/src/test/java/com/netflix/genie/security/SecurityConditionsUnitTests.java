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
package com.netflix.genie.security;

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
 * Tests for the Security Conditions.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class SecurityConditionsUnitTests {

    /**
     * Test the default constructor.
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(new SecurityConditions());
    }

    /**
     * Test the AnySecurityEnabled class.
     */
    @Test
    public void testAnySecurityEnabledConfiguration() {
        final SecurityConditions.AnySecurityEnabled anySecurityEnabled = new SecurityConditions.AnySecurityEnabled();
        Assert.assertThat(
            anySecurityEnabled.getConfigurationPhase(),
            Matchers.is(ConfigurationCondition.ConfigurationPhase.PARSE_CONFIGURATION)
        );
    }

    /**
     * Test to make sure that when no supported security is enabled the class doesn't fire.
     *
     * @throws Exception on any error
     */
    @Test
    public void cantEnableBeanWithoutAnySecurityEnabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(SecurityEnabled.class);
        Assert.assertFalse(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is enabled the class fires.
     *
     * @throws Exception on any error
     */
    @Test
    public void canEnableBeanWithSAMLEnabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            SecurityEnabled.class,
            "genie.security.saml.enabled:true",
            "genie.security.x509.enabled:false",
            "genie.security.oauth2.enabled:false"
        );
        Assert.assertTrue(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is enabled the class fires.
     *
     * @throws Exception on any error
     */
    @Test
    public void canEnableBeanWithX509Enabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            SecurityEnabled.class,
            "genie.security.saml.enabled:false",
            "genie.security.x509.enabled:true",
            "genie.security.oauth2.enabled:false"
        );
        Assert.assertTrue(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is enabled the class fires.
     *
     * @throws Exception on any error
     */
    @Test
    public void canEnableBeanWithOAuth2Enabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            SecurityEnabled.class,
            "genie.security.saml.enabled:false",
            "genie.security.x509.enabled:false",
            "genie.security.oauth2.enabled:true"
        );
        Assert.assertTrue(context.containsBean("myBean"));
        context.close();
    }

    /**
     * Test to make sure that when a supported security is enabled the class fires.
     *
     * @throws Exception on any error
     */
    @Test
    public void canEnableBeanWithAllSecurityEnabled() throws Exception {
        final AnnotationConfigApplicationContext context = this.load(
            SecurityEnabled.class,
            "genie.security.saml.enabled:true",
            "genie.security.x509.enabled:true",
            "genie.security.oauth2.enabled:true"
        );
        Assert.assertTrue(context.containsBean("myBean"));
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
     * Configuration class for testing AnySecurityEnabled.
     */
    @Configuration
    @Conditional(SecurityConditions.AnySecurityEnabled.class)
    public static class SecurityEnabled {

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
