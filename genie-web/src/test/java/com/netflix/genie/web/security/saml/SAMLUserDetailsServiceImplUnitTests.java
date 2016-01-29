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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.saml.SAMLCredential;

import java.util.UUID;

/**
 * Tests for the SAMLUserDetailsImpl class. Makes sure we're pulling the right attributes and authorities from the
 * SAML assertion.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class SAMLUserDetailsServiceImplUnitTests {

    private static final String USER_ATTRIBUTE_NAME = UUID.randomUUID().toString();
    private static final String GROUP_ATTRIBUTE_NAME = UUID.randomUUID().toString();
    private static final String ADMIN_GROUP = UUID.randomUUID().toString();

    private static final String USER_ID = UUID.randomUUID().toString();
    private static final String[] GROUPS = {UUID.randomUUID().toString(), ADMIN_GROUP, UUID.randomUUID().toString()};

    private SAMLUserDetailsServiceImpl service;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.service = new SAMLUserDetailsServiceImpl();
        this.service.adminGroup = ADMIN_GROUP;
        this.service.userAttributeName = USER_ATTRIBUTE_NAME;
        this.service.groupAttributeName = GROUP_ATTRIBUTE_NAME;
    }

    /**
     * Test to make sure a null credential throws exception.
     */
    @Test(expected = UsernameNotFoundException.class)
    public void doesThrowErrorOnNullCredential() {
        this.service.loadUserBySAML(null);
    }

    /**
     * Make sure if no username is found an exception is thrown.
     */
    @Test(expected = UsernameNotFoundException.class)
    public void doesThrowErrorOnUserIdNotFound() {
        final SAMLCredential credential = Mockito.mock(SAMLCredential.class);
        Mockito.when(credential.getAttributeAsString(Mockito.eq(USER_ATTRIBUTE_NAME))).thenReturn(null);
        this.service.loadUserBySAML(credential);
    }

    /**
     * Make sure if no groups are found but a user id is that the user logs in but only gets role user.
     */
    @Test
    public void canLoadUserWithoutGroups() {
        final SAMLCredential credential = Mockito.mock(SAMLCredential.class);
        Mockito.when(credential.getAttributeAsString(Mockito.eq(USER_ATTRIBUTE_NAME))).thenReturn(USER_ID);
        Mockito.when(credential.getAttributeAsStringArray(Mockito.eq(GROUP_ATTRIBUTE_NAME))).thenReturn(null);
        final Object result = this.service.loadUserBySAML(credential);

        Assert.assertThat(result, Matchers.notNullValue());
        Assert.assertTrue(result instanceof User);
        final User user = (User) result;
        Assert.assertThat(user.getUsername(), Matchers.is(USER_ID));
        Assert.assertThat(user.getAuthorities(), Matchers.contains(new SimpleGrantedAuthority("ROLE_USER")));
    }

    /**
     * Make sure if user logs in and has admin group they get admin rights.
     */
    @Test
    public void canLoadUserWithAdminGroup() {
        final SAMLCredential credential = Mockito.mock(SAMLCredential.class);
        Mockito.when(credential.getAttributeAsString(Mockito.eq(USER_ATTRIBUTE_NAME))).thenReturn(USER_ID);
        Mockito.when(credential.getAttributeAsStringArray(Mockito.eq(GROUP_ATTRIBUTE_NAME))).thenReturn(GROUPS);
        final Object result = this.service.loadUserBySAML(credential);

        Assert.assertThat(result, Matchers.notNullValue());
        Assert.assertTrue(result instanceof User);
        final User user = (User) result;
        Assert.assertThat(user.getUsername(), Matchers.is(USER_ID));
        Assert.assertThat(
            user.getAuthorities(),
            Matchers.hasItems(new SimpleGrantedAuthority("ROLE_USER"), new SimpleGrantedAuthority("ROLE_ADMIN"))
        );
    }

    /**
     * Make sure if user logs in and doesn't have admin group user only gets user role.
     */
    @Test
    public void canLoadUserWithoutAdminGroup() {
        final SAMLCredential credential = Mockito.mock(SAMLCredential.class);
        Mockito.when(credential.getAttributeAsString(Mockito.eq(USER_ATTRIBUTE_NAME))).thenReturn(USER_ID);
        Mockito.when(credential.getAttributeAsStringArray(Mockito.eq(GROUP_ATTRIBUTE_NAME)))
            .thenReturn(new String[]{UUID.randomUUID().toString(), UUID.randomUUID().toString()});
        final Object result = this.service.loadUserBySAML(credential);

        Assert.assertThat(result, Matchers.notNullValue());
        Assert.assertTrue(result instanceof User);
        final User user = (User) result;
        Assert.assertThat(user.getUsername(), Matchers.is(USER_ID));
        Assert.assertThat(
            user.getAuthorities(),
            Matchers.contains(new SimpleGrantedAuthority("ROLE_USER"))
        );
        Assert.assertThat(
            user.getAuthorities().size(),
            Matchers.is(1)
        );
    }
}
