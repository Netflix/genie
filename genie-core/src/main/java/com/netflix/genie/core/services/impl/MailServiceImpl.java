package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.MailService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/**
 * Implementation of the Mail service interface.
 *
 * @author amsharma
 * @since 3.0.0
 */
@ConditionalOnClass(JavaMailSender.class)
@ConditionalOnProperty("genie.mail.enabled")
@Component
public class MailServiceImpl implements MailService {

    private final JavaMailSender javaMailSender;
    private final String fromAddress;
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
    @Autowired
    public MailServiceImpl(
        final JavaMailSender javaMailSender,
        @Value("${genie.mail.fromAddress}")
        final String fromAddress,
        @Value("${genie.mail.user:#{null}}")
        final String mailUser,
        @Value("${genie.mail.password:#{null}}")
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
