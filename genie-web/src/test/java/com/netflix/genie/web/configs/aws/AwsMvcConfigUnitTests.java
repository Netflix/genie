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
package com.netflix.genie.web.configs.aws;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

/**
 * Unit tests for the AwsMvcConfig class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class AwsMvcConfigUnitTests {

    private AwsMvcConfig awsMvcConfig;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.awsMvcConfig = new AwsMvcConfig();
    }

    /**
     * Make sure we attempt public hostname first.
     */
    @Test
    public void canGetPublicHostname() {
        final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        final String hostname = UUID.randomUUID().toString();
        Mockito.when(restTemplate.getForObject(AwsMvcConfig.PUBLIC_HOSTNAME_GET, String.class)).thenReturn(hostname);

        Assert.assertThat(this.awsMvcConfig.hostName(restTemplate), Matchers.is(hostname));
        Mockito
            .verify(restTemplate, Mockito.times(1))
            .getForObject(AwsMvcConfig.PUBLIC_HOSTNAME_GET, String.class);
        Mockito
            .verify(restTemplate, Mockito.never())
            .getForObject(AwsMvcConfig.LOCAL_IPV4_HOSTNAME_GET, String.class);
    }

    /**
     * Make sure we get IPv4 hostname if public hostname fails.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canGetIPv4Hostname() {
        final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        final String hostname = UUID.randomUUID().toString();
        Mockito
            .when(restTemplate.getForObject(AwsMvcConfig.PUBLIC_HOSTNAME_GET, String.class))
            .thenThrow(RuntimeException.class);

        Mockito.when(restTemplate.getForObject(AwsMvcConfig.LOCAL_IPV4_HOSTNAME_GET, String.class))
            .thenReturn(hostname);

        Assert.assertThat(this.awsMvcConfig.hostName(restTemplate), Matchers.is(hostname));
        Mockito.verify(restTemplate, Mockito.times(1)).getForObject(AwsMvcConfig.PUBLIC_HOSTNAME_GET, String.class);
        Mockito.verify(restTemplate, Mockito.times(1)).getForObject(AwsMvcConfig.LOCAL_IPV4_HOSTNAME_GET, String.class);
    }

    /**
     * Make sure if both fails we throw an exception.
     */
    @Test(expected = RuntimeException.class)
    @SuppressWarnings("unchecked")
    public void cantGetHostname() {
        final RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        Mockito
            .when(restTemplate.getForObject(AwsMvcConfig.PUBLIC_HOSTNAME_GET, String.class))
            .thenThrow(RuntimeException.class);
        Mockito
            .when(restTemplate.getForObject(AwsMvcConfig.LOCAL_IPV4_HOSTNAME_GET, String.class))
            .thenThrow(RuntimeException.class);

        this.awsMvcConfig.hostName(restTemplate);
    }
}
