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

import com.netflix.genie.test.categories.UnitTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.lang.JoseException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.UUID;

/**
 * Unit tests for the PingFederateJWTConfig class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class PingFederateJWTConfigUnitTests {

    private static final String RSA_KEY = "-----BEGIN PUBLIC KEY-----\n"
        + "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCqGKukO1De7zhZj6+H0qtjTkVxwTCpvKe4eCZ0\n"
        + "FPqri0cb2JZfXJ/DgYSF6vUpwmJG8wVQZKjeGcjDOL5UlsuusFncCzWBQ7RKNUSesmQRMSGkVb1/\n"
        + "3j+skZ6UtW+5u09lHNsj6tQ51s1SPrCBkedbNf0Tp0GbMJDyR4e9T04ZZwIDAQAB\n"
        + "-----END PUBLIC KEY-----";

    private static final String PEM_CERT = "-----BEGIN CERTIFICATE-----\n"
        + "MIICVjCCAb8CAg37MA0GCSqGSIb3DQEBBQUAMIGbMQswCQYDVQQGEwJKUDEOMAwG\n"
        + "A1UECBMFVG9reW8xEDAOBgNVBAcTB0NodW8ta3UxETAPBgNVBAoTCEZyYW5rNERE\n"
        + "MRgwFgYDVQQLEw9XZWJDZXJ0IFN1cHBvcnQxGDAWBgNVBAMTD0ZyYW5rNEREIFdl\n"
        + "YiBDQTEjMCEGCSqGSIb3DQEJARYUc3VwcG9ydEBmcmFuazRkZC5jb20wHhcNMTIw\n"
        + "ODIyMDUyNzIzWhcNMTcwODIxMDUyNzIzWjBKMQswCQYDVQQGEwJKUDEOMAwGA1UE\n"
        + "CAwFVG9reW8xETAPBgNVBAoMCEZyYW5rNEREMRgwFgYDVQQDDA93d3cuZXhhbXBs\n"
        + "ZS5jb20wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAMYBBrx5PlP0WNI/ZdzD\n"
        + "+6Pktmurn+F2kQYbtc7XQh8/LTBvCo+P6iZoLEmUA9e7EXLRxgU1CVqeAi7QcAn9\n"
        + "MwBlc8ksFJHB0rtf9pmf8Oza9E0Bynlq/4/Kb1x+d+AyhL7oK9tQwB24uHOueHi1\n"
        + "C/iVv8CSWKiYe6hzN1txYe8rAgMBAAEwDQYJKoZIhvcNAQEFBQADgYEAASPdjigJ\n"
        + "kXCqKWpnZ/Oc75EUcMi6HztaW8abUMlYXPIgkV2F7YanHOB7K4f7OOLjiz8DTPFf\n"
        + "jC9UeuErhaA/zzWi8ewMTFZW/WshOrm3fNvcMrMLKtH534JKvcdMg6qIdjTFINIr\n"
        + "evnAhf0cwULaebn+lMs8Pdl7y37+sfluVok=\n"
        + "-----END CERTIFICATE-----";

    private final PingFederateJWTConfig config = new PingFederateJWTConfig();

    /**
     * Can get a validator.
     */
    @Test
    public void canGetValidator() {
        Assert.assertNotNull(this.config.pingFederateValidator(Mockito.mock(MeterRegistry.class)));
    }

    /**
     * Can get a public key.
     *
     * @throws IOException             On error
     * @throws JoseException           On error
     * @throws InvalidKeySpecException On error
     * @throws CertificateException    On error
     */
    @Test
    public void canGetPublicKey() throws IOException, JoseException, InvalidKeySpecException, CertificateException {
        try {
            this.config.jwtPublicKey("");
            Assert.fail();
        } catch (final IllegalArgumentException iae) {
            Assert.assertNotNull(iae);
        }

        try {
            this.config.jwtPublicKey(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final IllegalArgumentException iae) {
            Assert.assertNotNull(iae);
        }

        Assert.assertNotNull(this.config.jwtPublicKey(RSA_KEY));
        Assert.assertNotNull(this.config.jwtPublicKey(PEM_CERT));
    }

    /**
     * Can get the consumer.
     */
    @Test
    public void canGetJwtConsumer() {
        final PublicKey publicKey = Mockito.mock(PublicKey.class);
        final PingFederateValidator validator = Mockito.mock(PingFederateValidator.class);
        Assert.assertNotNull(this.config.jwtConsumer(publicKey, validator));
    }

    /**
     * Can get a token services instance.
     */
    @Test
    public void canGetTokenServices() {
        final JwtConsumer consumer = Mockito.mock(JwtConsumer.class);
        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        Assert.assertNotNull(this.config.pingFederateJWTTokenServices(consumer, registry));
    }
}
