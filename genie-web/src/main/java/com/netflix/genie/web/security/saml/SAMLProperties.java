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
package com.netflix.genie.web.security.saml;

import lombok.Data;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;

/**
 * Class to bind properties to for SAML configurations.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("genie.security.saml.enabled")
@ConfigurationProperties(prefix = "genie.security.saml")
@Component
@Data
public class SAMLProperties {

    @NotNull
    private Attributes attributes;
    @NotNull
    private Idp idp;
    @NotNull
    private Keystore keystore;
    @NotNull
    private Sp sp;

    /**
     * Contains attributes from the SAML assertion.
     *
     * @author tgianos
     * @since 3.0.0
     */
    @Data
    public static class Attributes {

        @NotNull
        private User user;
        @NotNull
        private Groups groups;

        /**
         * Attributes about the user.
         *
         * @author tgianos
         * @since 3.0.0
         */
        @Data
        public static class User {
            @NotBlank
            private String name;
        }

        /**
         * Contains attribute information regarding groups from the SAML assertion.
         *
         * @author tgianos
         * @since 3.0.0
         */
        @Data
        public static class Groups {
            @NotBlank
            private String name;
            @NotBlank
            private String admin;
        }
    }

    /**
     * Class containing information about the SAML IDP.
     *
     * @author tgianos
     * @since 3.0.0
     */
    @Data
    public static class Idp {
        @URL
        private String serviceProviderMetadataURL;
    }

    /**
     * Information about the keystore used to sign requests to the IDP.
     *
     * @author tgianos
     * @since 3.0.0
     */
    @Data
    public static class Keystore {
        @NotBlank
        private String name;
        @NotBlank
        private String password;
        @NotNull
        private DefaultKey defaultKey;

        /**
         * Information about the default key inside the keystore.
         *
         * @author tgianos
         * @since 3.0.0
         */
        @Data
        public static class DefaultKey {
            @NotBlank
            private String name;
            @NotBlank
            private String password;
        }
    }

    /**
     * Information about the service provider from the IDP.
     *
     * @author tgianos
     * @since 3.0.0
     */
    @Data
    public static class Sp {
        @NotBlank
        private String entityId;
        private String entityBaseURL;
    }
}
