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
package com.netflix.genie.web.configs;

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * Tests for the bean validation configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class ValidationConfigUnitTests {

    private ValidationConfig validationConfig;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.validationConfig = new ValidationConfig();
    }

    /**
     * Make sure the validation bean is of the right type.
     */
    @Test
    public void canGetValidator() {
        Assert.assertTrue(this.validationConfig.localValidatorFactoryBean() instanceof LocalValidatorFactoryBean);
    }

    /**
     * Make sure we get a method validation post processor.
     */
    @Test
    public void canGetMethodValidator() {
        Assert.assertNotNull(this.validationConfig.methodValidationPostProcessor());
    }

//    @Test
//    public void canDecrypt()
//        throws CertificateException, JoseException, InvalidKeySpecException, InvalidJwtException {
//        final String pubKeyString =
//            "-----BEGIN CERTIFICATE-----\n" +
//                "MIIECTCCAvGgAwIBAgIBATANBgkqhkiG9w0BAQsFADB4MQswCQYDVQQGEwJVUzETMBEGA1UECAwK" +
//                "Q2FsaWZvcm5pYTESMBAGA1UEBwwJTG9zIEdhdG9zMRMwEQYDVQQDDAptZWVjaHVtand0MRYwFAYD" +
//                "VQQKDA1OZXRmbGl4LCBJbmMuMRMwEQYDVQQLDApPcGVyYXRpb25zMB4XDTE2MDYyMDAwMDIxMloX" +
//                "DTIwMDYyMDIzNTgxMlowejEVMBMGA1UEAwwMTWVlY2h1bUpXVDAxMRYwFAYDVQQKDA1OZXRmbGl4" +
//                "LCBJbmMuMRMwEQYDVQQLDApPcGVyYXRpb25zMQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZv" +
//                "cm5pYTESMBAGA1UEBwwJTG9zIEdhdG9zMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA" +
//                "wV8q93rglCVXhjauq80g61urUjLKL2iMBt/wbxQ+oYaDp0F3mrLnBQQ9j5hwkCa5fOfq/oLArkAv" +
//                "6J3/wuPMDlhqGg4Fe1EooalB3cWLG9vfgc5hwKVoebxT/4FpyMKljBG0n19VUS1fd6BMMysHSGZ7" +
//                "BOLxSyNM/1472sMJPVQpXO0z09og+HYJhT631XVJI52iDXnty24NMwr85ymAt0ql3aQf0uwvBvfm" +
//                "RZTsV+AAU2GLrgM5ymP5g23/8KCdVfAY9wnUSi1ncjN+L3nzbCMlGCfnYGfJiVCIWb6jO0h8zOmU" +
//                "0AI8cIWYHDbWgniRpT8jzZ+NiuOS9qqmAQmWZwIDAQABo4GbMIGYMAwGA1UdEwEB/wQCMAAwCwYD" +
//                "VR0PBAQDAgWgMEcGA1UdHwRAMD4wPKA6oDiGNmh0dHA6Ly9wcm9kLmNsb3VkY2EuY3JsLm5ldGZs" +
//                "aXguY29tL01lZWNodW1KV1QvY3JsLnBlbTAdBgNVHQ4EFgQURkwcANdphiGde2qoYHmFgjM+k9Uw" +
//                "EwYDVR0lBAwwCgYIKwYBBQUHAwEwDQYJKoZIhvcNAQELBQADggEBABf3eAEZzoFkshOSxWLLzG6f" +
//                "B3yy9Gtc6TTCRDaZ1Y6HAHssIHEToRH0wS0rKxMvTXj41NgrU1loThx1NBlLLfKdVgq9fdbVEJbd" +
//                "0aH1gRhYBApI48bzPLNvyq0gXUCskj+rvkjTWqiX+gbpI2FNAu3OM3iMlYR3pBgesVXid6plXI/R" +
//                "HfgX6hgvqGX7KDFw0xZYZPHoFS0d4SzQcYlz6oozcQxjBPFMF9QYV6HIHe0rEnq7e3g5VGNHje4Q" +
//                "KBsPY5mJbAKP6jIhLY20a9ooCwk3+xvH3oQLxwv7OMJ/K5kBqsx8vioSa48rfEvU3UyxpTJb7bgY" +
//                "z3uqt1Y4QgFjpxo=\n" +
//                "-----END CERTIFICATE-----";
//
////        final String pubKeyString =
////            "-----BEGIN CERTIFICATE-----\n" +
////                "MIIDvzCCAqegAwIBAgIBBTANBgkqhkiG9w0BAQsFADB4MQswCQYDVQQGEwJVUzETMBEGA1UECAwK\n" +
////                "Q2FsaWZvcm5pYTESMBAGA1UEBwwJTG9zIEdhdG9zMRMwEQYDVQQDDAptZWVjaHVtand0MRYwFAYD\n" +
////                "VQQKDA1OZXRmbGl4LCBJbmMuMRMwEQYDVQQLDApPcGVyYXRpb25zMB4XDTE2MDcyODAwMDIwOFoX\n" +
////                "DTE4MDcyODIzNTgwOFowgYAxGzAZBgNVBAMMEk1lZWNodW1KV1QtR2VuaWUwMTEWMBQGA1UECgwN\n" +
////                "TmV0ZmxpeCwgSW5jLjETMBEGA1UECwwKT3BlcmF0aW9uczELMAkGA1UEBhMCVVMxEzARBgNVBAgM\n" +
////                "CkNhbGlmb3JuaWExEjAQBgNVBAcMCUxvcyBHYXRvczCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC\n" +
////                "AQoCggEBAKjgwLzGHllnX5+3cspKm80tYDFheIgsBvmVUUw6T/z9MF7Y1qqK2lK3kSIrzjGTs1qb\n" +
////                "nQxlg+57Ns+ENgRWFOwwobsa/BhD11kGkwAROXORKtNRaMM/Xx+wq/genpBccEK1SeHUG4V1a1OZ\n" +
////                "oAvmUPJVmWaq5PewN2dv98Bqu7K9zZtYmGHUKJadzxM3pSLGfr8u+5bTKZQppKRSdgQQwjU5mVj1\n" +
////                "Wljas4iLJJD72eo/vNHRIXT79x0kt8iSrfJSPPKgLd5dh+1S0KNRLTP98KuNUttoJUWd5qlzATbW\n" +
////                "NK2/cD6GDgzNyDXqijBSGr4M5cs4uJN0nwfi1kv54VMVW3kCAwEAAaNLMEkwRwYDVR0fBEAwPjA8\n" +
////                "oDqgOIY2aHR0cDovL3Byb2QuY2xvdWRjYS5jcmwubmV0ZmxpeC5jb20vTWVlY2h1bUpXVC9jcmwu\n" +
////                "cGVtMA0GCSqGSIb3DQEBCwUAA4IBAQAhChSEKmZHvHzaYGS2Qno/T7WL7FouXYVZvOQww0ze6u06\n" +
////                "ihV0Xise3qvWBVO47l8ycsU7/s9PEMBbdJ0hxaHwOsaxQPFkQM1q1xA7WWID84FmGl8mDc0NIvzu\n" +
////                "TklVOmHuuL6LtuplY7jj7td/GaJahMTI2193+daAn3VRwca+Gp5IL2P7DaEdsc40Xwnqb8qoamMX\n" +
////                "6d211p5g5yNkSQT+L09sYsmMZWp8ks30ZR3njPO/A4gBCuVSSGum6fX7jHu6pIbfmH5zL1Y+d1+m\n" +
////                "j1BjR6wQjWXanahpn65KIU55useXgQVoznZYCI1Xf/8h9HJOtxfkI9PRLkhUaP6TBTsf\n" +
////                "-----END CERTIFICATE-----";
//
////        final X509Util keyUtil = new X509Util();
////        final PublicKey publicKey = keyUtil.fromBase64Der(pubKeyString).getPublicKey();
//
//        CertificateFactory fact = CertificateFactory.getInstance("X.509");
//        X509Certificate cer
//            = (X509Certificate) fact.generateCertificate(new ByteArrayInputStream(pubKeyString.getBytes()));
//        PublicKey publicKey = cer.getPublicKey();
//
//        final JwtConsumer consumer = new JwtConsumerBuilder()
//            .setVerificationKey(publicKey)
//            .setExpectedIssuer("https://meechum.netflix.com")
//            .setRequireExpirationTime()
//            .build();
//
//        final String jwt = "eyJhbGciOiJSUzUxMiIsImtpZCI6Im5mbHhKd3REZWZhdWx0IiwieDV0IjoiUWFnalY" +
//            "wanVqOVZvQVlQUmtyZFRXUXRHMnE0In0.eyJjbGllbnRfaWQiOiJuZmx4LWdlbmllLWp3dC1jbGllbnQiLCJ" +
//            "leHAiOjE0NzAxNjUyMjMsInNjb3BlIjpbImdlbmllX3VzZXIiXSwiaXNzIjoiaHR0cHM6Ly9tZWVjaHVtLm5ldG" +
//            "ZsaXguY29tIn0.MET8PTJf6KGJkKNTf6DjRk-D673p71iI_xYCTPD04nRwK5G0tmvmhslsRhJs-X6vCohn1mTHwV" +
//            "og1A1-hiyS-Mav4vCQXQPJoaLQBWu4U4bk6MBfoHB6tnez3CKT0havDC39PgguPh6z-Rv4MIW5YJ2tzJ9M8oeL" +
//            "XjRyNi90wsKB7UHB5uAwWNR00Flzz3y9DnoF0D7EAJy3SDeceNksXEGf6kuMop-72Qu3uCW_i_A64rsZq9W33Lqq" +
//            "TwtwYGWjCZ5QdOlrcixfokP39CfOZ3iu7UqR3x8oBvc-moQt9ZfXtIEKWab0DcjvrWtqEEoB1LKhQpIsgP6uYskunqBvdA";
//
//        final JwtClaims claims = consumer.processToClaims(jwt);
//
//        final Map<String, List<Object>> flatClaims = claims.flattenClaims();
//        flatClaims.get("scope");
//    }
}
