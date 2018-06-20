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
package com.netflix.genie.security.x509;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import java.util.HashMap;
import java.util.UUID;

/**
 * Tests for X509UserDetailsService.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class X509UserDetailsServiceUnitTests {

    private X509UserDetailsService service;
    private PreAuthenticatedAuthenticationToken token;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.service = new X509UserDetailsService();
        this.token = Mockito.mock(PreAuthenticatedAuthenticationToken.class);
    }

    /**
     * Make sure if the principal isn't found it throws an exception.
     *
     * @throws UsernameNotFoundException on principal not found
     */
    @Test(expected = UsernameNotFoundException.class)
    public void cantAuthenticateWithoutPrincipal() throws UsernameNotFoundException {
        Mockito.when(this.token.getPrincipal()).thenReturn(null);
        this.service.loadUserDetails(this.token);
    }

    /**
     * Make sure if the principal isn't a string it throws an exception.
     *
     * @throws UsernameNotFoundException on principal not a string
     */
    @Test(expected = UsernameNotFoundException.class)
    public void cantAuthenticateWithNonStringPrincipal() throws UsernameNotFoundException {
        Mockito.when(this.token.getPrincipal()).thenReturn(new HashMap<String, String>());
        this.service.loadUserDetails(this.token);
    }

    /**
     * Make sure if the principal doesn't split properly it fails.
     *
     * @throws UsernameNotFoundException on principal not containing both username and roles
     */
    @Test(expected = UsernameNotFoundException.class)
    public void cantAuthenticateWithoutUsernameAndRoles() throws UsernameNotFoundException {
        Mockito.when(this.token.getPrincipal()).thenReturn("donthaveroles");
        this.service.loadUserDetails(this.token);
    }

    /**
     * Make sure if the roles are empty it fails.
     *
     * @throws UsernameNotFoundException on on principal roles section being empty
     */
    @Test(expected = UsernameNotFoundException.class)
    public void cantAuthenticateWithoutRoleString() throws UsernameNotFoundException {
        Mockito.when(this.token.getPrincipal()).thenReturn("donthaveroles:");
        this.service.loadUserDetails(this.token);
    }

    /**
     * Make sure if the roles are empty it fails.
     *
     * @throws UsernameNotFoundException on on principal roles section being empty
     */
    @Test(expected = UsernameNotFoundException.class)
    public void cantAuthenticateWithoutRoles() throws UsernameNotFoundException {
        Mockito.when(this.token.getPrincipal()).thenReturn("donthaveroles:,");
        this.service.loadUserDetails(this.token);
    }

    /**
     * Make sure if everything is present and proper the service returns a valid user.
     *
     * @throws UsernameNotFoundException on any error
     */
    @Test
    public void canAuthenticate() throws UsernameNotFoundException {
        final String username = UUID.randomUUID().toString();
        final String role1 = UUID.randomUUID().toString();
        final String role2 = UUID.randomUUID().toString();
        Mockito.when(this.token.getPrincipal()).thenReturn(username + ":" + role1 + "," + role2);
        final UserDetails userDetails = this.service.loadUserDetails(this.token);

        if (!(userDetails instanceof User)) {
            throw new UsernameNotFoundException("Invalid return type");
        }

        final User user = (User) userDetails;
        Assert.assertThat(user.getUsername(), Matchers.is(username));
        Assert.assertThat(user.getPassword(), Matchers.is("NA"));
        Assert.assertThat(user.getAuthorities().size(), Matchers.is(3));
        Assert.assertThat(
            user.getAuthorities(),
            Matchers.hasItems(
                new SimpleGrantedAuthority("ROLE_USER"),
                new SimpleGrantedAuthority("ROLE_" + role1.toUpperCase()),
                new SimpleGrantedAuthority("ROLE_" + role2.toUpperCase())
            )
        );
    }
}
