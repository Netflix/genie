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
package com.netflix.genie.security.saml;

import com.google.common.collect.Lists;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Get the user and roles from a SAML certificate.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty(value = "genie.security.saml.enabled", havingValue = "true")
@Component
@Slf4j
public class SAMLUserDetailsServiceImpl implements SAMLUserDetailsService {

    private static final GrantedAuthority USER = new SimpleGrantedAuthority("ROLE_USER");
    private static final GrantedAuthority ADMIN = new SimpleGrantedAuthority("ROLE_ADMIN");

    private final SAMLProperties samlProperties;
    private final Timer loadTimer;

    /**
     * Constructor.
     *
     * @param samlProperties The SAML properties to use
     * @param registry       The metrics registry to use
     */
    @Autowired
    public SAMLUserDetailsServiceImpl(
        @NotNull final SAMLProperties samlProperties,
        @NotNull final MeterRegistry registry
    ) {
        this.samlProperties = samlProperties;
        this.loadTimer = registry.timer("genie.security.saml.parse.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object loadUserBySAML(final SAMLCredential credential) throws UsernameNotFoundException {
        final long start = System.nanoTime();
        try {
            if (credential == null) {
                throw new UsernameNotFoundException("No credential entered. Unable to get username.");
            }

            final String userAttributeName = this.samlProperties.getAttributes().getUser().getName();
            final String userId = credential.getAttributeAsString(userAttributeName);
            if (StringUtils.isBlank(userId)) {
                throw new UsernameNotFoundException("No user id found using attribute: " + userAttributeName);
            }

            // User exists. Give them at least USER role
            final List<GrantedAuthority> authorities = Lists.newArrayList(USER);

            // See if we can get any other roles
            final String groupAttributeName = this.samlProperties.getAttributes().getGroups().getName();
            final String adminGroup = this.samlProperties.getAttributes().getGroups().getAdmin();
            final String[] groups = credential.getAttributeAsStringArray(groupAttributeName);
            if (groups == null) {
                log.warn("No groups found. User will only get ROLE_USER by default.");
            } else if (Arrays.asList(groups).contains(adminGroup)) {
                authorities.add(ADMIN);
            }

            // For debugging what's available in the credential from the IDP
            if (log.isDebugEnabled()) {
                log.debug("Attributes:");
                credential.getAttributes().forEach(attribute -> {
                    log.debug("Attribute: {}", attribute.getName());
                    log.debug(
                        "Values: {}",
                        StringUtils.join(credential.getAttributeAsStringArray(attribute.getName()), ',')
                    );
                });
            }

            log.info("{} is logged in with authorities {}", userId, authorities);
            return new User(userId, "DUMMY", authorities);
        } finally {
            final long finished = System.nanoTime();
            this.loadTimer.record(finished - start, TimeUnit.NANOSECONDS);
        }
    }
}
