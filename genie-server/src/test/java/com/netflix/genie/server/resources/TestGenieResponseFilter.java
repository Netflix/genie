/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.server.resources;

/**
 * Tests for the GenieResponseFilter.
 *
 * @author tgianos
 */
public class TestGenieResponseFilter {
//
//    private static final ContainerRequestContext REQUEST = Mockito.mock(ContainerRequestContext.class);
//    private GenieNodeStatistics statistics;
//    private GenieResponseFilter genieResponseFilter;
//    private ContainerResponseContext response;
//
//    /**
//     * Setup for tests.
//     */
//    @Before
//    public void setup() {
//        this.statistics = Mockito.mock(GenieNodeStatistics.class);
//        this.response = Mockito.mock(ContainerResponseContext.class);
//        this.genieResponseFilter = new GenieResponseFilter(this.statistics);
//    }
//
//    /**
//     * Test 200's response codes.
//     */
//    @Test
//    public void testFilter200s() {
//        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_OK);
//        this.genieResponseFilter.filter(REQUEST, this.response);
//        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_CREATED);
//        this.genieResponseFilter.filter(REQUEST, this.response);
//
//        Mockito.verify(this.statistics, Mockito.times(2)).incrGenie2xxCount();
//        Mockito.verify(this.statistics, Mockito.never()).incrGenie4xxCount();
//        Mockito.verify(this.statistics, Mockito.never()).incrGenie5xxCount();
//    }
//
//    /**
//     * Test 400's response codes.
//     */
//    @Test
//    public void testFilter400s() {
//        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
//        this.genieResponseFilter.filter(REQUEST, this.response);
//        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_PRECON_FAILED);
//        this.genieResponseFilter.filter(REQUEST, this.response);
//
//        Mockito.verify(this.statistics, Mockito.never()).incrGenie2xxCount();
//        Mockito.verify(this.statistics, Mockito.times(2)).incrGenie4xxCount();
//        Mockito.verify(this.statistics, Mockito.never()).incrGenie5xxCount();
//    }
//
//    /**
//     * Test 500's response codes.
//     */
//    @Test
//    public void testFilter500s() {
//        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
//        this.genieResponseFilter.filter(REQUEST, this.response);
//
//        Mockito.verify(this.statistics, Mockito.never()).incrGenie2xxCount();
//        Mockito.verify(this.statistics, Mockito.never()).incrGenie4xxCount();
//        Mockito.verify(this.statistics, Mockito.times(1)).incrGenie5xxCount();
//    }
}
