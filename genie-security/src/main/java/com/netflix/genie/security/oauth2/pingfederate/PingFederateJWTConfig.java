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

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.RsaKeyUtil;
import org.jose4j.lang.JoseException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;

/**
 * Beans needed to support OAuth2 authentication via JWT tokens returned from Ping Federate.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@Conditional(PingFederateSecurityConditions.PingFederateJWTEnabled.class)
public class PingFederateJWTConfig {

    /**
     * A validator which checks the validity of the JWT tokens sent in from ping federate against expected
     * Genie required fields.
     *
     * @param registry The metrics registry to use
     * @return The validator
     */
    @Bean
    public PingFederateValidator pingFederateValidator(final MeterRegistry registry) {
        return new PingFederateValidator(registry);
    }

    /**
     * The public key used to verify the signatures of JWT tokens.
     *
     * @param keyValue The string of the public key to use in either RSA or X.509 format
     * @return A public key object to use when validating JWT tokens
     * @throws IOException             On reading or closing byte array input stream
     * @throws JoseException           When trying to create the key using jose library
     * @throws InvalidKeySpecException When the cert has an invalid spec
     * @throws CertificateException    When trying to create a X.509 specification object
     */
    @Bean
    public PublicKey jwtPublicKey(@Value("${genie.security.oauth2.pingfederate.jwt.keyValue}") final String keyValue)
        throws IOException, JoseException, InvalidKeySpecException, CertificateException {
        final String certBegin = "-----BEGIN CERTIFICATE-----";
        final String rsaBegin = "-----BEGIN PUBLIC KEY-----";
        if (StringUtils.isEmpty(keyValue)) {
            // In future try a key resolver to pull the key from the server
            throw new IllegalArgumentException("No value set for security.oauth2.resource.jwt.keyValue");
        }

        if (keyValue.startsWith(certBegin)) {
            // X.509 cert
            try (final ByteArrayInputStream bis = new ByteArrayInputStream(keyValue.getBytes("UTF-8"))) {
                final CertificateFactory fact = CertificateFactory.getInstance("X.509");
                final X509Certificate cer = (X509Certificate) fact.generateCertificate(bis);
                return cer.getPublicKey();
            }
        } else if (keyValue.startsWith(rsaBegin)) {
            // RSA Public Key
            return new RsaKeyUtil().fromPemEncoded(keyValue);
        } else {
            throw new IllegalArgumentException(
                "Only support X.509 pem certs or Public RSA Keys for security.oauth2.resource.jwt.keyValue"
            );
        }
    }

    /**
     * The jwtConsumer class which will be used to verify and parse the JWT token from ping federate.
     *
     * @param jwtPublicKey          The public key used to verify the signature on the JWT token.
     * @param pingFederateValidator The validator to add to the validation chain specifically for Ping Federate
     * @return The consumer to use
     */
    @Bean
    public JwtConsumer jwtConsumer(
        @Qualifier("jwtPublicKey") final PublicKey jwtPublicKey,
        final PingFederateValidator pingFederateValidator
    ) {
        return new JwtConsumerBuilder()
            .setVerificationKey(jwtPublicKey)
            .setRequireExpirationTime()
            .registerValidator(pingFederateValidator)
            .build();
    }

    /**
     * The token services class used to take a JWT token and produce a Spring Security Authentication object.
     *
     * @param jwtConsumer The JWT consumer used to verify and parse the JWT tokens
     * @param registry    The metrics registry to use for collecting metrics
     * @return The Token services class
     */
    @Bean
    @Primary
    public PingFederateJWTTokenServices pingFederateJWTTokenServices(
        final JwtConsumer jwtConsumer,
        final MeterRegistry registry
    ) {
        return new PingFederateJWTTokenServices(jwtConsumer, registry);
    }
}
