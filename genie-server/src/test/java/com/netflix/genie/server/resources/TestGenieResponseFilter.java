package com.netflix.genie.server.resources;

import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.HttpURLConnection;

/**
 * Tests for the GenieResponseFilter.
 *
 * @author tgianos
 */
public class TestGenieResponseFilter {

    private GenieNodeStatistics statistics;
    private GenieResponseFilter genieResponseFilter;
    private ContainerResponse response;
    private static final ContainerRequest REQUEST = Mockito.mock(ContainerRequest.class);

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.statistics = Mockito.mock(GenieNodeStatistics.class);
        this.response = Mockito.mock(ContainerResponse.class);
        this.genieResponseFilter = new GenieResponseFilter(this.statistics);
    }

    /**
     * Test 200's response codes.
     */
    @Test
    public void testFilter200s() {
        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_OK);
        this.genieResponseFilter.filter(REQUEST, this.response);
        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_CREATED);
        this.genieResponseFilter.filter(REQUEST, this.response);

        Mockito.verify(this.statistics, Mockito.times(2)).incrGenie2xxCount();
        Mockito.verify(this.statistics, Mockito.never()).incrGenie4xxCount();
        Mockito.verify(this.statistics, Mockito.never()).incrGenie5xxCount();
    }

    /**
     * Test 400's response codes.
     */
    @Test
    public void testFilter400s() {
        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
        this.genieResponseFilter.filter(REQUEST, this.response);
        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_PRECON_FAILED);
        this.genieResponseFilter.filter(REQUEST, this.response);

        Mockito.verify(this.statistics, Mockito.never()).incrGenie2xxCount();
        Mockito.verify(this.statistics, Mockito.times(2)).incrGenie4xxCount();
        Mockito.verify(this.statistics, Mockito.never()).incrGenie5xxCount();
    }

    /**
     * Test 500's response codes.
     */
    @Test
    public void testFilter500s() {
        Mockito.when(this.response.getStatus()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);
        this.genieResponseFilter.filter(REQUEST, this.response);

        Mockito.verify(this.statistics, Mockito.never()).incrGenie2xxCount();
        Mockito.verify(this.statistics, Mockito.never()).incrGenie4xxCount();
        Mockito.verify(this.statistics, Mockito.times(1)).incrGenie5xxCount();
    }
}
