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
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Get the user and roles from a SAML certificate.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("security.saml.enabled")
@Service
public class SAMLUserDetailsServiceImpl implements SAMLUserDetailsService {

    private static final Logger LOG = LoggerFactory.getLogger(SAMLUserDetailsServiceImpl.class);

    @Value("${security.saml.attributes.user.name}")
    private String userAttributeName;
    @Value("${security.saml.attributes.groups.name}")
    private String groupAttributeName;
    @Value("${security.saml.attributes.groups.admin}")
    private String adminGroup;

    /**
     * {@inheritDoc}
     */
    @Override
    public Object loadUserBySAML(final SAMLCredential credential) throws UsernameNotFoundException {
//        final String userId = credential.getNameID().getValue();
        final String userId = credential.getAttributeAsString(this.userAttributeName);
        final String[] groups = credential.getAttributeAsStringArray(this.groupAttributeName);

        LOG.info("{} is logged in", userId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Attributes:");
            credential.getAttributes().stream().forEach(attribute -> {
                LOG.debug("Attribute: {}", attribute.getName());
                LOG.debug(
                    "Values: {}",
                    StringUtils.arrayToCommaDelimitedString(credential.getAttributeAsStringArray(attribute.getName()))
                );
            });
        }

        final List<GrantedAuthority> authorities = Lists.newArrayList(new SimpleGrantedAuthority("ROLE_USER"));

        if (Arrays.asList(groups).contains(this.adminGroup)) {
            authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
        }

        return new User(userId, "DUMMY", authorities);
    }
}
