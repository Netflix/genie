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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.MailService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

/**
 * Implementation of the Mail service interface.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class MailServiceImpl implements MailService {

    private final JavaMailSender javaMailSender;
    private final String fromAddress;
    // TODO can we use this here?
    private final String mailUser;
    private final String mailPassword;

    /**
     * Constructor.
     *
     * @param javaMailSender An implementation of the JavaMailSender interface.
     * @param fromAddress The from email address for the email.
     * @param mailUser The userid of the account used to send email.
     * @param mailPassword The password of the account used to send email.
     *
     * @throws GenieException If there is any problem.
     */
    public MailServiceImpl(
        final JavaMailSender javaMailSender,
        final String fromAddress,
        final String mailUser,
        final String mailPassword
    ) throws GenieException {
        this.javaMailSender = javaMailSender;
        this.fromAddress = fromAddress;
        this.mailUser = mailUser;
        this.mailPassword = mailPassword;
    }

    @Override
    public void sendEmail(
        @NotBlank(message = "Cannot send email to blank address.")
        final String toEmail,
        @NotBlank(message = "Subject cannot be empty")
        final String subject,
        final String body
    ) throws GenieException {

        final SimpleMailMessage simpleMailMessage = new SimpleMailMessage();

        simpleMailMessage.setTo(toEmail);
        simpleMailMessage.setFrom(fromAddress);
        simpleMailMessage.setSubject(subject);

        // check if body is not empty
        if (StringUtils.isNotBlank(body)) {
            simpleMailMessage.setText(body);
        }

        try {
            javaMailSender.send(simpleMailMessage);
        } catch (MailException me) {
            throw new GenieServerException("Failure to send email: " + me);
        }
    }
}
