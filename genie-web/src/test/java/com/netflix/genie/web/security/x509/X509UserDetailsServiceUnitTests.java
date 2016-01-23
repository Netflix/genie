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

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

/**
 * Tests for X509UserDetailsService.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class X509UserDetailsServiceUnitTests {

    /**
     * Just a dummy test for now until there is an actual implementation. Adding this so it fails and
     * reminds us to write test later.
     *
     * @throws UsernameNotFoundException on error
     */
    @Test(expected = UsernameNotFoundException.class)
    public void cantLoadUserDetails() throws UsernameNotFoundException {
        new X509UserDetailsService().loadUserDetails(Mockito.mock(PreAuthenticatedAuthenticationToken.class));
    }
}
