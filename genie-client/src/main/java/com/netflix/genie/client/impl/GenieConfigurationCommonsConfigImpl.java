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
package com.netflix.genie.client.impl;

import com.netflix.genie.client.GenieConfiguration;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;

/**
 * An implementation of the GenieConfiguration interface using Apache Commons Configuration.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class GenieConfigurationCommonsConfigImpl implements GenieConfiguration {

    private static final String SERVICE_URL_KEY = "genie.service.url";

    private static final String GENIE_SECURITY_ENABLED_KEY = "genie.security.enabled";
    private static final String GENIE_SECURITY_OAUTH_URL_KEY = "genie.security.oauth.url";
    private static final String GENIE_SECURITY_OAUTH_CLIENT_ID_KEY = "genie.security.oauth.clientId";
    private static final String GENIE_SECURITY_OAUTH_CLIENT_SECRET_KEY = "genie.security.oauth.clientSecret";
    private static final String GENIE_SECURITY_OAUTH_GRANT_TYPE_KEY = "genie.security.oauth.grantType";
    private static final String GENIE_SECURITY_OAUTH_SCOPE_KEY = "genie.security.oauth.scope";

    private final Configuration configuration;

    /**
     * Constructor.
     *
     * @param configuration A apache commons configuration object.
     *
     * @throws GenieException If there is a problem.
     */
    public GenieConfigurationCommonsConfigImpl(
        final Configuration configuration
        ) throws GenieException {
        if (configuration != null) {
            this.configuration = configuration;
        } else {
            throw  new GeniePreconditionException("Configuration object cannot be null.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getServiceUrl() {
        return configuration.getString(SERVICE_URL_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setServiceUrl(final String url) throws GenieException {
        if (StringUtils.isBlank(url)) {
            throw new GeniePreconditionException("Url cannot be empty or null");
        }
        configuration.setProperty(SERVICE_URL_KEY, url);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean isSecurityEnabled() {
        return configuration.getBoolean(GENIE_SECURITY_ENABLED_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSecurityEnabled(final Boolean flag) throws GenieException {
        configuration.setProperty(GENIE_SECURITY_ENABLED_KEY, flag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getOauthUrl() {
        return configuration.getString(GENIE_SECURITY_OAUTH_URL_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setOauthUrl(final String url) throws GenieException {
        if (StringUtils.isBlank(url)) {
            throw new GeniePreconditionException("Url cannot be empty or null");
        }
        configuration.setProperty(GENIE_SECURITY_OAUTH_URL_KEY, url);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientId() {
        return configuration.getString(GENIE_SECURITY_OAUTH_CLIENT_ID_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientId(final String clientId) throws GenieException {
        if (StringUtils.isBlank(clientId)) {
            throw new GeniePreconditionException("ClientId cannot be empty or null");
        }
        configuration.setProperty(GENIE_SECURITY_OAUTH_CLIENT_ID_KEY, clientId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientSecret() {
        return configuration.getString(GENIE_SECURITY_OAUTH_CLIENT_SECRET_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientSecret(final String clientSecret) throws GenieException {
        if (StringUtils.isBlank(clientSecret)) {
            throw new GeniePreconditionException("ClientId cannot be empty or null");
        }
        configuration.setProperty(GENIE_SECURITY_OAUTH_CLIENT_SECRET_KEY, clientSecret);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getGrantType() {
        return configuration.getString(GENIE_SECURITY_OAUTH_GRANT_TYPE_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setGrantType(final String grantType) throws GenieException {
        if (StringUtils.isBlank(grantType)) {
            throw new GeniePreconditionException("Grant Type cannot be empty or null");
        }
        configuration.setProperty(GENIE_SECURITY_OAUTH_GRANT_TYPE_KEY, grantType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getScope() {
        return configuration.getString(GENIE_SECURITY_OAUTH_SCOPE_KEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setScope(final String scope) throws GenieException {
        if (StringUtils.isBlank(scope)) {
            throw new GeniePreconditionException("Scope cannot be empty or null");
        }
        configuration.setProperty(GENIE_SECURITY_OAUTH_SCOPE_KEY, scope);
    }
}
