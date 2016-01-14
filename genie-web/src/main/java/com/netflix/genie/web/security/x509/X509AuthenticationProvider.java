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
package com.netflix.genie.web.security.x509;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

/**
 * An authentication provider for x509 client certificates.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class X509AuthenticationProvider implements AuthenticationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(X509AuthenticationProvider.class);

    /*
      otherName                       [0]
      rfc822Name                      [1]
      dNSName                         [2]
      x400Address                     [3]
      directoryName                   [4]
      ediPartyName                    [5]
      uniformResourceIdentifier       [6]
      iPAddress                       [7]
      registeredID                    [8]
    */
    private static final String RFC822_NAME_ID = "1";

    /**
     * {@inheritDoc}
     */
    @Override
    public Authentication authenticate(final Authentication authentication) throws AuthenticationException {
        LOG.info("\n\n\nENTERING X509AuthenticationProvider authenticate...\n\n\n");
        if (!(authentication.getCredentials() instanceof X509Certificate)) {
            return null;
        }

        final X509Certificate cert = (X509Certificate) authentication.getCredentials();
        try {
            final Collection<List<?>> subjectAlternativeNames = cert.getSubjectAlternativeNames();
            if (subjectAlternativeNames == null) {
                throw new BadCredentialsException("No subject alternative names found. Unable to use cert");
            }

            // TODO: fix authorities etc.
            String rfc822Name = null;
            for (final List<?> subjectAlternativeName : subjectAlternativeNames) {
                if (subjectAlternativeName.get(0).equals(RFC822_NAME_ID)) {
                    rfc822Name = (String) subjectAlternativeName.get(1);
                }
            }

            if (rfc822Name == null) {
                rfc822Name = authentication.getPrincipal().toString();
            }

            final List<GrantedAuthority> authorities = Lists.newArrayList(new SimpleGrantedAuthority("ROLE_USER"));
            return new PreAuthenticatedAuthenticationToken(
                new User(rfc822Name, "JUNK", authorities),
                authentication.getCredentials()
            );
        } catch (final CertificateParsingException cpe) {
            throw new BadCredentialsException(cpe.getMessage(), cpe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supports(final Class<?> authentication) {
        return authentication.isAssignableFrom(PreAuthenticatedAuthenticationToken.class);
    }
}
