/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.configs;

import com.netflix.genie.web.properties.MailProperties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.mail.javamail.JavaMailSender;

/**
 * Tests for {@link GenieMailAutoConfiguration} configuration.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieMailAutoConfigurationUnitTests {

    /**
     * Confirm we can get a default mail service implementation.
     */
    @Test
    public void canGetDefaultMailServiceImpl() {
        Assert.assertNotNull(new GenieMailAutoConfiguration().getDefaultMailServiceImpl());
    }

    /**
     * Confirm we can get a mail service implementation using JavaMailSender.
     */
    @Test
    public void canGetMailServiceImpl() {
        final JavaMailSender javaMailSender = Mockito.mock(JavaMailSender.class);
        final MailProperties properties = new MailProperties();
        properties.setFromAddress("test@genie.com");
        Assert.assertNotNull(
            new GenieMailAutoConfiguration().getJavaMailSenderMailService(javaMailSender, properties)
        );
    }
}
