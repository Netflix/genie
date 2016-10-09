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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.MailService;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
@Category(UnitTest.class)
public class MailServiceImplUnitTests {

    private MailService mailService;
    private JavaMailSender mailSender;
    private String fromAddress;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.mailSender = Mockito.mock(JavaMailSender.class);
        this.fromAddress = UUID.randomUUID().toString();
        this.mailService = new MailServiceImpl(this.mailSender, this.fromAddress);
    }

    /**
     * Make sure we can successfully send an email.
     *
     * @throws GenieException On error
     */
    @Test
    public void canSendEmailWithBody() throws GenieException {
        final ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        final String to = UUID.randomUUID().toString();
        final String subject = UUID.randomUUID().toString();
        final String body = UUID.randomUUID().toString();

        this.mailService.sendEmail(to, subject, body);
        Mockito.verify(this.mailSender, Mockito.times(1)).send(captor.capture());
        Assert.assertThat(captor.getValue().getTo()[0], Matchers.is(to));
        Assert.assertThat(captor.getValue().getFrom(), Matchers.is(this.fromAddress));
        Assert.assertThat(captor.getValue().getSubject(), Matchers.is(subject));
        Assert.assertThat(captor.getValue().getText(), Matchers.is(body));
    }

    /**
     * Make sure we can successfully send an email.
     *
     * @throws GenieException On error
     */
    @Test
    public void canSendEmailWithoutBody() throws GenieException {
        final ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        final String to = UUID.randomUUID().toString();
        final String subject = UUID.randomUUID().toString();

        this.mailService.sendEmail(to, subject, null);
        Mockito.verify(this.mailSender, Mockito.times(1)).send(captor.capture());
        Assert.assertThat(captor.getValue().getTo()[0], Matchers.is(to));
        Assert.assertThat(captor.getValue().getFrom(), Matchers.is(this.fromAddress));
        Assert.assertThat(captor.getValue().getSubject(), Matchers.is(subject));
        Assert.assertThat(captor.getValue().getText(), Matchers.nullValue());
    }

    /**
     * Make sure if we can't send an email an exception is thrown.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieServerException.class)
    public void cantSendEmail() throws GenieException {
        final String to = UUID.randomUUID().toString();
        final String subject = UUID.randomUUID().toString();
        final String body = UUID.randomUUID().toString();

        Mockito.doThrow(new MailSendException("a")).when(this.mailSender).send(Mockito.any(SimpleMailMessage.class));
        this.mailService.sendEmail(to, subject, body);
    }
}
