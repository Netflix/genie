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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.services.MailService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.util.UUID;

/**
 * Tests for the MailServiceImpl class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class MailServiceImplTest {

    private MailService mailService;
    private JavaMailSender mailSender;
    private String fromAddress;

    @BeforeEach
    void setup() {
        this.mailSender = Mockito.mock(JavaMailSender.class);
        this.fromAddress = UUID.randomUUID().toString();
        this.mailService = new MailServiceImpl(this.mailSender, this.fromAddress);
    }

    @Test
    void canSendEmailWithBody() throws GenieException {
        final ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        final String to = UUID.randomUUID().toString();
        final String subject = UUID.randomUUID().toString();
        final String body = UUID.randomUUID().toString();

        this.mailService.sendEmail(to, subject, body);
        Mockito.verify(this.mailSender, Mockito.times(1)).send(captor.capture());
        Assertions
            .assertThat(captor.getValue())
            .isNotNull()
            .extracting(SimpleMailMessage::getTo)
            .isNotNull()
            .isEqualTo(new String[]{to});
        Assertions.assertThat(captor.getValue().getFrom()).isEqualTo(this.fromAddress);
        Assertions.assertThat(captor.getValue().getSubject()).isEqualTo(subject);
        Assertions.assertThat(captor.getValue().getText()).isEqualTo(body);
    }

    @Test
    void canSendEmailWithoutBody() throws GenieException {
        final ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        final String to = UUID.randomUUID().toString();
        final String subject = UUID.randomUUID().toString();

        this.mailService.sendEmail(to, subject, null);
        Mockito.verify(this.mailSender, Mockito.times(1)).send(captor.capture());
        Assertions
            .assertThat(captor.getValue())
            .isNotNull()
            .extracting(SimpleMailMessage::getTo)
            .isNotNull()
            .isEqualTo(new String[]{to});
        Assertions.assertThat(captor.getValue().getFrom()).isEqualTo(this.fromAddress);
        Assertions.assertThat(captor.getValue().getSubject()).isEqualTo(subject);
        Assertions.assertThat(captor.getValue().getText()).isNull();
    }

    @Test
    void cantSendEmail() {
        final String to = UUID.randomUUID().toString();
        final String subject = UUID.randomUUID().toString();
        final String body = UUID.randomUUID().toString();

        Mockito.doThrow(new MailSendException("a")).when(this.mailSender).send(Mockito.any(SimpleMailMessage.class));
        Assertions
            .assertThatExceptionOfType(GenieServerException.class)
            .isThrownBy(() -> this.mailService.sendEmail(to, subject, body));
    }
}
