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
import com.netflix.genie.web.services.MailService;
import com.netflix.genie.web.services.impl.DefaultMailServiceImpl;
import com.netflix.genie.web.services.impl.MailServiceImpl;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.mail.MailSenderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;

/**
 * Auto configuration for email support within Genie.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@AutoConfigureAfter(MailSenderAutoConfiguration.class)
@EnableConfigurationProperties(
    {
        MailProperties.class
    }
)
public class GenieMailAutoConfiguration {

    /**
     * Returns a bean for mail service impl using the Spring Mail.
     *
     * @param javaMailSender      An implementation of the JavaMailSender interface.
     * @param genieMailProperties The Genie specific properties for email notifications
     * @return An instance of MailService implementation.
     */
    @Bean
    @ConditionalOnBean(JavaMailSender.class)
    public MailService getJavaMailSenderMailService(
        final JavaMailSender javaMailSender,
        final MailProperties genieMailProperties
    ) {
        return new MailServiceImpl(javaMailSender, genieMailProperties.getFromAddress());
    }

    /**
     * Get an default implementation of the Mail Service interface if nothing is supplied.
     *
     * @return The mail service implementation that does nothing.
     */
    @Bean
    @ConditionalOnMissingBean(JavaMailSender.class)
    public MailService getDefaultMailServiceImpl() {
        return new DefaultMailServiceImpl();
    }
}
