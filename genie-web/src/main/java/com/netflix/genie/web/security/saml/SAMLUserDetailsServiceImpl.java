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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Get the user and roles from a SAML certificate.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("security.saml.enabled")
@Component
public class SAMLUserDetailsServiceImpl implements SAMLUserDetailsService {

    private static final Logger LOG = LoggerFactory.getLogger(SAMLUserDetailsServiceImpl.class);

    private static final GrantedAuthority USER = new SimpleGrantedAuthority("ROLE_USER");
    private static final GrantedAuthority ADMIN = new SimpleGrantedAuthority("ROLE_ADMIN");

    @Value("${security.saml.attributes.user.name}")
    protected String userAttributeName;
    @Value("${security.saml.attributes.groups.name}")
    protected String groupAttributeName;
    @Value("${security.saml.attributes.groups.admin}")
    protected String adminGroup;

    /**
     * {@inheritDoc}
     */
    @Override
    public Object loadUserBySAML(final SAMLCredential credential) throws UsernameNotFoundException {
        if (credential == null) {
            throw new UsernameNotFoundException("No credential entered. Unable to get username.");
        }

        //final String userId = credential.getNameID().getValue();
        final String userId = credential.getAttributeAsString(this.userAttributeName);
        if (StringUtils.isBlank(userId)) {
            throw new UsernameNotFoundException("No user id found using attribute: " + this.userAttributeName);
        }

        // User exists. Give them at least USER role
        final List<GrantedAuthority> authorities = Lists.newArrayList(USER);

        // See if we can get any other roles
        final String[] groups = credential.getAttributeAsStringArray(this.groupAttributeName);
        if (groups == null) {
            LOG.warn("No groups found. User will only get ROLE_USER by default.");
        } else if (Arrays.asList(groups).contains(this.adminGroup)) {
            authorities.add(ADMIN);
        }

        // For debugging what's available in the credential from the IDP
        if (LOG.isDebugEnabled()) {
            LOG.debug("Attributes:");
            credential.getAttributes().stream().forEach(attribute -> {
                LOG.debug("Attribute: {}", attribute.getName());
                LOG.debug(
                    "Values: {}",
                    StringUtils.join(credential.getAttributeAsStringArray(attribute.getName()), ',')
                );
            });
        }

        LOG.info("{} is logged in with authorities {}", userId, authorities);
        return new User(userId, "DUMMY", authorities);
    }
}
