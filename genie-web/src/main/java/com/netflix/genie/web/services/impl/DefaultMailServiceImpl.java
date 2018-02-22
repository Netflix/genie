/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.services.MailService;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

/**
 * Default No-Op implementation of Mail Service Interface.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class DefaultMailServiceImpl implements MailService {
    /**
     * Method to send emails.
     *
     * @param toEmail The email address to send the email to.
     * @param subject The subject of the email.
     * @param body    The body of the email
     * @throws GenieException If there is any problem
     */
    @Override
    public void sendEmail(
        @NotBlank(message = "Cannot send email to blank address.")
        @Nonnull final String toEmail,
        @NotBlank(message = "Subject cannot be empty")
        @Nonnull final String subject,
        @Nullable final String body
    ) throws GenieException {
        log.debug("Default mail service skips sending email.");
    }
}
