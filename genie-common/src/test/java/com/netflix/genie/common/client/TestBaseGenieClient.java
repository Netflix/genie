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
package com.netflix.genie.common.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.client.ClientException;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Job;
import com.netflix.niws.client.http.RestClient;
import com.sun.jersey.api.client.Client;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Test the BaseGenieClient functionality.
 *
 * @author tgianos
 */
public class TestBaseGenieClient {

    private RestClient restClient;
    private BaseGenieClient client;
    private HttpRequest request;
    private HttpResponse response;
    private static final Application APPLICATION = new Application(
            "Firefly",
            "Malcolm",
            "1.0",
            ApplicationStatus.ACTIVE
    );
    private static String eurekaEnv = null;

    /**
     * Setup variables that will be used constantly across tests.
     */
    @BeforeClass
    public static void setupClass() {
        //Make sure eureka is never set
        eurekaEnv = System.getProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY);
        if (eurekaEnv != null) {
            System.clearProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY);
        }
    }

    /**
     * Setup variables to use in the tests.
     *
     * @throws IOException during mock creation.
     */
    @Before
    public void setup() throws IOException {
        this.restClient = Mockito.mock(RestClient.class);
        this.client = new BaseGenieClient(restClient);
        this.request = Mockito.mock(HttpRequest.class);
        this.response = Mockito.mock(HttpResponse.class);
    }

    /**
     * Clear out any remaining things that may have been set in tests.
     */
    @AfterClass
    public static void tearDownClass() {
        //Make sure eureka is never set
        if (eurekaEnv != null) {
            System.setProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY, eurekaEnv);
        } else {
            System.clearProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY);
        }
    }

    /**
     * Test the default constructor behavior.
     *
     * @throws IOException during creation.
     */
    @Test
    public void testConstructorNoParam() throws IOException {
        Assert.assertNotNull(new BaseGenieClient(null));
    }

    /**
     * Test the constructor with passing in a client.
     */
    @Test
    public void testConstructorWithParam() {
        Assert.assertEquals(this.restClient, this.client.getRestClient());
        Mockito.verify(this.restClient, Mockito.times(1)).setJerseyClient(Mockito.any(Client.class));
    }

    /**
     * Test to make sure http request is never sent if no return type is entered.
     *
     * @throws GenieException Random issues.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testExecuteRequestNoReturnTypeEntered() throws GenieException {
        this.client.executeRequest(this.request, null, null);
    }

    /**
     * Test to make sure http request is never sent if no entity class is entered along with a collection.
     *
     * @throws GenieException Random issues.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testExecuteRequestNoEntityClassEnteredForCollection() throws GenieException {
        this.client.executeRequest(this.request, Set.class, null);
    }

    /**
     * Test to make sure http request is never sent if no http request is entered.
     *
     * @throws GenieException Random issues.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testExecuteRequestNoRequestEntered() throws GenieException {
        this.client.executeRequest(null, Set.class, String.class);
    }

    /**
     * Test to make sure if response is null a genie exception is thrown not a NPE.
     *
     * @throws GenieException  Random issues.
     * @throws ClientException Some http request failed.
     */
    @Test(expected = GenieServerException.class)
    public void testExecuteRequestNoResponseReturned()
            throws GenieException, ClientException {
        Mockito.when(this.restClient.executeWithLoadBalancer(this.request)).thenReturn(null);
        this.client.executeRequest(this.request, Set.class, String.class);
    }

    /**
     * Test to make sure if response isn't success an Exception is thrown.
     *
     * @throws Exception
     */
    @Test(expected = GenieException.class)
    public void testExecuteRequestNotSuccessful() throws Exception {
        Mockito.when(this.response.isSuccess()).thenReturn(false);
        Mockito.when(this.response.getStatus()).thenReturn(500);
        Mockito.when(this.response.getEntity(String.class)).thenReturn("Server error");
        Mockito.when(this.restClient.executeWithLoadBalancer(this.request)).thenReturn(this.response);
        this.client.executeRequest(this.request, Set.class, String.class);
    }

    /**
     * Test to make sure if response is successful entity is returned.
     *
     * @throws GenieException  Random issues.
     * @throws ClientException A http client.
     * @throws IOException     IOException.
     */
    @Test
    public void testExecuteRequestSuccessSingleEntity()
            throws GenieException, ClientException, IOException {
        Mockito.when(this.response.isSuccess()).thenReturn(true);

        final ObjectMapper mapper = new ObjectMapper();
        final StringWriter writer = new StringWriter();
        mapper.writeValue(writer, APPLICATION);
        final String inputEntity = writer.toString();
        final InputStream is = new ByteArrayInputStream(inputEntity.getBytes(Charset.forName("UTF-8")));
        Mockito.when(this.response.getInputStream()).thenReturn(is);

        Mockito.when(this.restClient.executeWithLoadBalancer(this.request)).thenReturn(this.response);

        final Application outputEntity =
                (Application) this.client.executeRequest(this.request, null, Application.class);

        Assert.assertEquals(APPLICATION.getName(), outputEntity.getName());
        Assert.assertEquals(APPLICATION.getUser(), outputEntity.getUser());
        Assert.assertEquals(APPLICATION.getVersion(), outputEntity.getVersion());
        Assert.assertEquals(APPLICATION.getStatus(), outputEntity.getStatus());

        Mockito.verify(this.response, Mockito.times(1)).isSuccess();
        Mockito.verify(this.response, Mockito.times(1)).getInputStream();
    }

    /**
     * Test to make sure if response is successful collection is returned.
     *
     * @throws GenieException             Random issues.
     * @throws ClientException            A http client.
     * @throws IOException                IOException.
     */
    @Test
    public void testExecuteRequestSuccessCollection()
            throws GenieException, ClientException, IOException {
        Mockito.when(this.response.isSuccess()).thenReturn(true);

        final List<Command> commands = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            commands.add(
                    new Command(
                            "name" + i,
                            "user" + i,
                            "" + i,
                            CommandStatus.ACTIVE,
                            "executable" + i
                    )
            );
        }

        final ObjectMapper mapper = new ObjectMapper();
        final StringWriter writer = new StringWriter();
        mapper.writeValue(writer, commands);
        final String inputEntity = writer.toString();
        final InputStream is = new ByteArrayInputStream(inputEntity.getBytes(Charset.forName("UTF-8")));
        Mockito.when(this.response.getInputStream()).thenReturn(is);
        Mockito.when(this.restClient.executeWithLoadBalancer(this.request)).thenReturn(this.response);

        @SuppressWarnings("unchecked")
        final List<Command> outputCommands = (List<Command>) this.client.executeRequest(
                this.request, List.class, Command.class);

        Assert.assertEquals(commands.size(), outputCommands.size());
        for (int i = 0; i < commands.size(); i++) {
            final Command expected = commands.get(i);
            final Command actual = outputCommands.get(i);
            Assert.assertEquals(expected.getName(), actual.getName());
            Assert.assertEquals(expected.getUser(), actual.getUser());
            Assert.assertEquals(expected.getStatus(), actual.getStatus());
            Assert.assertEquals(expected.getExecutable(), actual.getExecutable());
            Assert.assertEquals(expected.getVersion(), actual.getVersion());
        }

        Mockito.verify(this.response, Mockito.times(1)).isSuccess();
        Mockito.verify(this.response, Mockito.times(1)).getInputStream();
    }

    /**
     * Test to make sure when you build a request and pass in null for the verb it doesn't work.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBuildRequestNoVerb() throws GenieException {
        BaseGenieClient.buildRequest(null, "blah", null, null);
    }

    /**
     * Test to make sure when you build a request and don't pass in entity with post it fails.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBuildRequestNullEntityForPost() throws GenieException {
        BaseGenieClient.buildRequest(HttpRequest.Verb.POST, null, null, null);
    }

    /**
     * Test to make sure when you build a request and don't pass in entity with post it fails.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBuildRequestNullEntityForPut() throws GenieException {
        BaseGenieClient.buildRequest(HttpRequest.Verb.PUT, null, null, null);
    }

    /**
     * Test to make sure when you build a request and don't pass URI it fails.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBuildRequestNullRequestUri() throws GenieException {
        BaseGenieClient.buildRequest(HttpRequest.Verb.PUT, null, null, new Job());
    }

    /**
     * Test to make sure when you build a request and don't pass URI it fails.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBuildRequestEmptyRequestUri() throws GenieException {
        BaseGenieClient.buildRequest(HttpRequest.Verb.PUT, "", null, new Job());
    }

    /**
     * Test to make sure when you build a request and don't pass URI it fails.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBuildRequestBlankRequestUri() throws GenieException {
        BaseGenieClient.buildRequest(HttpRequest.Verb.PUT, "   ", null, new Job());
    }

    /**
     * Test to make sure when you send a bad URI.
     *
     * @throws GenieException On any error.
     */
    @Test(expected = GenieException.class)
    public void testBuildRequestBadUri() throws GenieException {
        BaseGenieClient.buildRequest(HttpRequest.Verb.PUT, "I am not a valid URI", null, new Job());
    }

    /**
     * Test to make sure builds a valid post request.
     *
     * @throws GenieException On any error.
     */
    @Test
    public void testBuildRequestValidPost() throws GenieException {
        final String uri = "http://localhost:7001/genie/v2/jobs";
        final Job job = new Job();
        final HttpRequest validRequest = BaseGenieClient.buildRequest(
                HttpRequest.Verb.POST, uri, null, job);

        Assert.assertEquals(HttpRequest.Verb.POST, validRequest.getVerb());
        Assert.assertEquals(uri, validRequest.getUri().toString());
        Assert.assertEquals(job, validRequest.getEntity());
        Assert.assertEquals(1, validRequest.getHeaders().get(HttpHeaders.CONTENT_TYPE).size());
        Assert.assertTrue(validRequest.getHeaders().get(HttpHeaders.CONTENT_TYPE).contains(MediaType.APPLICATION_JSON));
        Assert.assertEquals(1, validRequest.getHeaders().get(HttpHeaders.ACCEPT).size());
        Assert.assertTrue(validRequest.getHeaders().get(HttpHeaders.ACCEPT).contains(MediaType.APPLICATION_JSON));
        Assert.assertTrue(validRequest.getQueryParams().isEmpty());
    }

    /**
     * Test to make sure builds a valid get request with no query parameters.
     *
     * @throws GenieException On any error.
     */
    @Test
    public void testBuildRequestValidGetEmptyQueryParams() throws GenieException {
        final String uri = "http://localhost:7001/genie/v2/jobs";
        final Multimap<String, String> queryParams = ArrayListMultimap.create();
        final HttpRequest validRequest = BaseGenieClient.buildRequest(
                HttpRequest.Verb.GET, uri, queryParams, null);

        Assert.assertEquals(HttpRequest.Verb.GET, validRequest.getVerb());
        Assert.assertEquals(uri, validRequest.getUri().toString());
        Assert.assertNull(validRequest.getEntity());
        Assert.assertEquals(1, validRequest.getHeaders().get(HttpHeaders.CONTENT_TYPE).size());
        Assert.assertTrue(validRequest.getHeaders().get(HttpHeaders.CONTENT_TYPE).contains(MediaType.APPLICATION_JSON));
        Assert.assertEquals(1, validRequest.getHeaders().get(HttpHeaders.ACCEPT).size());
        Assert.assertTrue(validRequest.getHeaders().get(HttpHeaders.ACCEPT).contains(MediaType.APPLICATION_JSON));
        Assert.assertTrue(validRequest.getQueryParams().isEmpty());
    }

    /**
     * Test to make sure builds a valid get request with no query parameters.
     *
     * @throws GenieException On any error.
     */
    @Test
    public void testBuildRequestValidGetWithSomeQueryParams() throws GenieException {
        final String uri = "http://localhost:7001/genie/v2/jobs";
        final Multimap<String, String> queryParams = ArrayListMultimap.create();
        queryParams.put("key1", "value1");
        queryParams.put("key1", "value2");
        queryParams.put("key2", "value1");
        final HttpRequest validRequest = BaseGenieClient.buildRequest(
                HttpRequest.Verb.GET, uri, queryParams, null);

        Assert.assertEquals(HttpRequest.Verb.GET, validRequest.getVerb());
        Assert.assertEquals(uri, validRequest.getUri().toString());
        Assert.assertNull(validRequest.getEntity());
        Assert.assertEquals(1, validRequest.getHeaders().get(HttpHeaders.CONTENT_TYPE).size());
        Assert.assertTrue(validRequest.getHeaders().get(HttpHeaders.CONTENT_TYPE).contains(MediaType.APPLICATION_JSON));
        Assert.assertEquals(1, validRequest.getHeaders().get(HttpHeaders.ACCEPT).size());
        Assert.assertTrue(validRequest.getHeaders().get(HttpHeaders.ACCEPT).contains(MediaType.APPLICATION_JSON));
        Assert.assertFalse(validRequest.getQueryParams().isEmpty());
        Assert.assertEquals(2, validRequest.getQueryParams().get("key1").size());
        Assert.assertTrue(validRequest.getQueryParams().get("key1").contains("value1"));
        Assert.assertTrue(validRequest.getQueryParams().get("key1").contains("value2"));
        Assert.assertEquals(1, validRequest.getQueryParams().get("key2").size());
        Assert.assertTrue(validRequest.getQueryParams().get("key2").contains("value1"));
    }

    /**
     * Try to test the init eureka method.
     */
    @Test
    public void testInitEurekaNullEnvironment() {
        Assert.assertNull(System.getProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY));
        BaseGenieClient.initEureka(null);
        Assert.assertNull(System.getProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY));
    }

    /**
     * Try to test the init eureka method.
     */
    @Test
    public void testInitEureka() {
        final String env = "dev";
        Assert.assertNull(System.getProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY));
        BaseGenieClient.initEureka(env);
        Assert.assertNotNull(System.getProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY));
        Assert.assertEquals(env, System.getProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY));
        System.clearProperty(BaseGenieClient.EUREKA_ENVIRONMENT_PROPERTY);
    }
}
