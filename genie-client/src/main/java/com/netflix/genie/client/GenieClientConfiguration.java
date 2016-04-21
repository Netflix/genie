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
package com.netflix.genie.client;

import com.netflix.genie.common.exceptions.GenieException;

/**
 * An interface to provide all the configurations needed for constructing a Genie Client.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface GenieClientConfiguration {

    /**
     * Get the URL for the Genie service.
     *
     * @return The url of the genie service.
     */
    String getServiceUrl();

    /**
     * Set the URL for the Genie service.
     *
     * @param url The url of the genie service.
     *
     * @throws GenieException In case of any problems.
     */
    void setServiceUrl(final String url) throws GenieException;

    /**** OAuth Security related properties    ****/

    /**
     * Flag to determine if security is enabled or not.
     *
     * @return True or False based on whether security is enabled on the server.
     */
    Boolean isSecurityEnabled();

    /**
     * Method to set the securityEnabled flag.
     *
     * @param flag The value to set.
     *
     * @throws GenieException In case of any problems.
     */
    void setSecurityEnabled(final Boolean flag) throws GenieException;

    /**
     * Get the URL of the OAuth IDP server.
     *
     * @return The URL.
     */
    String getSecurityOauthUrl();

    /**
     * Sets the Url for the OAUth IDP server.
     *
     * @param url The url value.
     *
     * @throws GenieException In case of any problems.
     */
    void setSecurityOauthUrl(final String url) throws GenieException;

    /**
     * Get the clientId for retrieving the OAuth credentials from the server.
     *
     * @return The clientId for the OAuth server.
     */
    String getSecurityClientId();

    /**
     * Sets the clientId to use with the OAuth server.
     *
     * @param clientId The client id value.
     *
     * @throws GenieException In case of any problems.
     */
    void setSecurityClientId(final String clientId) throws GenieException;

    /**
     * Get the client secret for retrieving the OAuth credentials from the server.
     *
     * @return The client secret for the OAuth server.
     */
    String getSecurityClientSecret();

    /**
     * Sets the client secret to use with the OAuth server.
     *
     * @param clientSecret The client id value.
     *
     * @throws GenieException In case of any problems.
     */
    void setSecurityClientSecret(final String clientSecret) throws GenieException;

    /**
     * Get the grant Type for retrieving the OAuth credentials from the server.
     *
     * @return The grant type for the OAuth server.
     */
    String getSecurityGrantType();

    /**
     * Sets the grant type to use with the OAuth server.
     *
     * @param grantType The client id value.
     *
     * @throws GenieException In case of any problems.
     */
    void setSecurityGrantType(final String grantType) throws GenieException;

    /**
     * Get the scope for retrieving the OAuth credentials from the server.
     *
     * @return The scope for the OAuth server.
     */
    String getSecurityScope();

    /**
     * Sets the scope to use with the OAuth server.
     *
     * @param scope The client id value.
     *
     * @throws GenieException In case of any problems.
     */
    void setSecurityScope(final String scope) throws GenieException;
}

