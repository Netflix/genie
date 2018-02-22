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
package com.netflix.genie.web.services;

import com.netflix.genie.common.exceptions.GenieException;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;

/**
 * An interface for sending emails.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Validated
public interface MailService {

    /**
     * Method to send emails.
     *
     * @param toEmail The email address to send the email to.
     * @param subject The subject of the email.
     * @param body    The body of the email
     * @throws GenieException If there is any problem
     */
    void sendEmail(
        @NotBlank(message = "Cannot send email to blank address.")
        @Nonnull final String toEmail,
        @NotBlank(message = "Subject cannot be empty")
        @Nonnull final String subject,
        @Nullable final String body
    ) throws GenieException;
}
