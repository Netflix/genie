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

import com.netflix.genie.test.categories.UnitTest;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.parse.ParserPool;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.saml.SAMLAuthenticationProvider;
import org.springframework.security.saml.context.SAMLContextProvider;
import org.springframework.security.saml.context.SAMLContextProviderLB;
import org.springframework.security.saml.key.JKSKeyManager;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.metadata.CachingMetadataManager;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;
import org.springframework.security.saml.websso.WebSSOProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileOptions;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;

import java.util.UUID;

/**
 * Unit tests for the SAMLConfig class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class SAMLConfigUnitTests {

    private SAMLConfig config;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.config = new SAMLConfig();
    }

    /**
     * Make sure we can get a valid SAMLBootstrap object.
     */
    @Test
    public void canGetSAMLBootstrap() {
        Assert.assertNotNull(SAMLConfig.samlBootstrap());
    }

    /**
     * Make sure we can get a valid VelocityEngine object.
     */
    @Test
    public void canGetVelocityEngine() {
        final VelocityEngine velocityEngine = this.config.velocityEngine();
        Assert.assertNotNull(velocityEngine);
        Assert.assertThat(velocityEngine.getProperty(RuntimeConstants.ENCODING_DEFAULT), Matchers.is("UTF-8"));
    }

    /**
     * Make sure we get a valid parser pool.
     */
    @Test
    public void canGetParserPool() {
        Assert.assertNotNull(this.config.parserPool());
    }

    /**
     * Make sure we can get a valid parser pool holder.
     */
    @Test
    public void canGetParserPoolHolder() {
        Assert.assertNotNull(this.config.parserPoolHolder());
    }

    /**
     * Make sure we can get a valid http connection manager.
     */
    @Test
    public void canGetMultiThreadedHttpConnectionManager() {
        Assert.assertNotNull(this.config.multiThreadedHttpConnectionManager());
    }

    /**
     * Make sure we can get a valid http client.
     */
    @Test
    public void canGetHttpClient() {
        final HttpClient client = this.config.httpClient();
        Assert.assertNotNull(client);
        Assert.assertTrue(client.getHttpConnectionManager() instanceof MultiThreadedHttpConnectionManager);
    }

    /**
     * Make sure we can get a valid SAMLAuthenticationProvider.
     */
    @Test
    public void canGetSAMLAuthenticationProvider() {
        final SAMLUserDetailsServiceImpl service = Mockito.mock(SAMLUserDetailsServiceImpl.class);
        final SAMLAuthenticationProvider provider = this.config.samlAuthenticationProvider(service);
        Assert.assertNotNull(provider);
        Assert.assertThat(provider.getUserDetails(), Matchers.is(service));
        Assert.assertFalse(provider.isForcePrincipalAsString());
    }

    /**
     * Make sure we can get a valid SAMLContextProvider.
     */
    @Test
    public void canGetContextProvider() {
        final SAMLProperties properties = Mockito.mock(SAMLProperties.class);

        Mockito.when(properties.getLoadBalancer()).thenReturn(null);

        SAMLContextProvider provider = this.config.contextProvider(properties);
        Assert.assertNotNull(provider);
        Assert.assertFalse(provider instanceof SAMLContextProviderLB);

        final SAMLProperties.LoadBalancer loadBalancer = new SAMLProperties.LoadBalancer();
        final String scheme = UUID.randomUUID().toString();
        loadBalancer.setScheme(scheme);
        final String serverName = UUID.randomUUID().toString();
        loadBalancer.setServerName(serverName);
        final int port = 443;
        loadBalancer.setServerPort(port);
        final String contextPath = UUID.randomUUID().toString();
        loadBalancer.setContextPath(contextPath);

        Mockito.when(properties.getLoadBalancer()).thenReturn(loadBalancer);
        provider = this.config.contextProvider(properties);
        Assert.assertNotNull(provider);
        Assert.assertTrue(provider instanceof SAMLContextProviderLB);
    }

    /**
     * Make sure we can get a valid saml logger.
     */
    @Test
    public void canGetSAMLLogger() {
        Assert.assertNotNull(this.config.samlLogger());
    }

    /**
     * Make sure we can get a valid web sso profile consumer.
     */
    @Test
    public void canGetWebSSOProfileConsumer() {
        Assert.assertTrue(this.config.webSSOprofileConsumer() instanceof WebSSOProfileConsumerImpl);
    }

    /**
     * Make sure we can get a valid web sso hok consumer.
     */
    @Test
    public void canGetWebSSOProfileConsumerForHok() {
        Assert.assertNotNull(this.config.hokWebSSOprofileConsumer());
    }

    /**
     * Make sure we can get a valid web sso profile.
     */
    @Test
    public void canGetWebSSOProfile() {
        Assert.assertTrue(this.config.webSSOprofile() instanceof WebSSOProfileImpl);
    }

    /**
     * Make sure we can get a valid hok web sso profile.
     */
    @Test
    public void canGetHokWebSSOProfile() {
        Assert.assertNotNull(this.config.hokWebSSOProfile());
    }

    /**
     * Make sure we can get a valid ECP profile.
     */
    @Test
    public void canGetEcpProfile() {
        Assert.assertNotNull(this.config.ecpprofile());
    }

    /**
     * Make sure we can get a valid logout profile.
     */
    @Test
    public void canGetLogoutProfile() {
        Assert.assertNotNull(this.config.logoutProfile());
    }

    /**
     * Make sure we can get a valid key manager.
     */
    @Test
    public void canGetKeyManager() {
        this.setupForKeyManager();

        final KeyManager keyManager = this.config.keyManager();
        Assert.assertTrue(keyManager instanceof JKSKeyManager);
    }

    /**
     * Make sure we can get a valid Web SSO profile options.
     */
    @Test
    public void canGetWebSSOProfileOptions() {
        final WebSSOProfileOptions options = this.config.defaultWebSSOProfileOptions();
        Assert.assertFalse(options.isIncludeScoping());
    }

    /**
     * Make sure we can get a valid SAMLEntryPoint for authentication.
     */
    @Test
    public void canGetSAMLEntryPoint() {
        Assert.assertNotNull(this.config.samlEntryPoint());
    }

    /**
     * Make sure we can get a valid extended metadata object.
     */
    @Test
    public void canGetExtendedMetadata() {
        Assert.assertNotNull(this.config.extendedMetadata());
    }

    /**
     * Make sure we can get a valid saml IDP discovery object.
     */
    @Test
    public void canGetSAMLIDPDiscovery() {
        Assert.assertNotNull(this.config.samlIDPDiscovery());
    }

    /**
     * Make sure we can get a valid extended metadata delegate.
     *
     * @throws MetadataProviderException on exception
     */
    @Test
    public void canGetExtendedMetdataDelegate() throws MetadataProviderException {
        final SAMLProperties properties = Mockito.mock(SAMLProperties.class);

        final String metadataUrl = UUID.randomUUID().toString();
        final SAMLProperties.Idp idp = Mockito.mock(SAMLProperties.Idp.class);
        Mockito.when(idp.getServiceProviderMetadataURL()).thenReturn(metadataUrl);
        Mockito.when(properties.getIdp()).thenReturn(idp);

        Assert.assertNotNull(this.config.ssoCircleExtendedMetadataProvider(properties));
    }

    /**
     * Make sure we can get a valid metadata manager.
     *
     * @throws MetadataProviderException on exception
     */
    @Test
    public void canGetMetadata() throws MetadataProviderException {
        final SAMLProperties properties = Mockito.mock(SAMLProperties.class);
        this.config.setSamlProperties(properties);

        final String metadataUrl = UUID.randomUUID().toString();
        final SAMLProperties.Idp idp = Mockito.mock(SAMLProperties.Idp.class);
        Mockito.when(idp.getServiceProviderMetadataURL()).thenReturn(metadataUrl);
        Mockito.when(properties.getIdp()).thenReturn(idp);

        final ExtendedMetadataDelegate extendedMetadataDelegate = Mockito.mock(ExtendedMetadataDelegate.class);

        final CachingMetadataManager metadataManager = this.config.metadata(extendedMetadataDelegate);
        Assert.assertNotNull(metadataManager);
        Assert.assertThat(metadataManager.getAvailableProviders().size(), Matchers.is(1));
        Assert.assertThat(metadataManager.getAvailableProviders(), Matchers.hasItem(extendedMetadataDelegate));
    }

    /**
     * Make sure we can get a valid metadata generator.
     *
     * @throws MetadataProviderException on exception
     */
    @Test
    public void canGetMetadataGenerator() throws MetadataProviderException {
        final SAMLProperties properties = this.setupForMetadataGenerator();

        final MetadataGenerator generator = this.config.metadataGenerator();
        Assert.assertNotNull(generator);
        Assert.assertThat(generator.getEntityId(), Matchers.is(properties.getSp().getEntityId()));
        Assert.assertFalse(generator.isIncludeDiscoveryExtension());
    }

    /**
     * Make sure we can get a valid metadata display filter.
     */
    @Test
    public void canGetMetadataDisplayFilter() {
        Assert.assertNotNull(this.config.metadataDisplayFilter());
    }

    /**
     * Make sure we can get a valid successful redirect handler.
     */
    @Test
    public void canGetSavedRequestAwareAuthenticationSuccessHandler() {
        Assert.assertNotNull(this.config.successRedirectHandler());
    }

    /**
     * Make sure we can get a valid authentication failure handler.
     */
    @Test
    public void canGetAuthenticationFailureHandler() {
        Assert.assertNotNull(this.config.authenticationFailureHandler());
    }

    /**
     * Make sure we can get a Web SSO HoK processing filter.
     *
     * @throws Exception on any problem
     */
    @Ignore
    @Test
    public void canGetSAMLWebSSOHoKProcessingFilter() throws Exception {
        Assert.assertNotNull(this.config.samlWebSSOHoKProcessingFilter());
    }

    /**
     * Make sure we can get a Web SSO processing filter.
     *
     * @throws Exception on any problem
     */
    @Ignore
    @Test
    public void canGetSAMLWebSSOProcessingFilter() throws Exception {
        Assert.assertNotNull(this.config.samlWebSSOProcessingFilter());
    }

    /**
     * Make sure we can get metadata generator filter.
     */
    @Test
    public void canGetMetadataGeneratorFilter() {
        this.setupForMetadataGenerator();
        Assert.assertNotNull(config.metadataGeneratorFilter());
    }

    /**
     * Make sure we can get a valid logout handler.
     */
    @Test
    public void canGetSuccessLogoutHandler() {
        Assert.assertNotNull(this.config.successLogoutHandler());
    }

    /**
     * Make sure we can get a valid security context logout handler.
     */
    @Test
    public void canGetSecurityContextLogoutHandler() {
        final SecurityContextLogoutHandler handler = this.config.logoutHandler();
        Assert.assertNotNull(handler);
        Assert.assertTrue(handler.isInvalidateHttpSession());
    }

    /**
     * Make sure we can get a valid SAML logout processing filter.
     */
    @Test
    public void canGetSAMLLogoutProcessingFilter() {
        Assert.assertNotNull(this.config.samlLogoutProcessingFilter());
    }

    /**
     * Make sure can get a valid saml logout filter.
     */
    @Test
    public void canGetSAMLLogoutFilter() {
        Assert.assertNotNull(this.config.samlLogoutFilter());
    }

    /**
     * Make sure can get a valid artifact binding.
     */
    @Test
    public void canGetArtifactBinding() {
        final ParserPool pool = Mockito.mock(ParserPool.class);
        final VelocityEngine engine = Mockito.mock(VelocityEngine.class);
        Assert.assertNotNull(this.config.artifactBinding(pool, engine));
    }

    /**
     * Make sure can get valid SOAP binding.
     */
    @Test
    public void canGetSOAPBinding() {
        Assert.assertNotNull(this.config.soapBinding());
    }

    /**
     * Make sure can get a valid Http Post binding.
     */
    @Test
    public void canGetPostBinding() {
        Assert.assertNotNull(this.config.httpPostBinding());
    }

    /**
     * Make sure we can get a valid redirect deflate binding.
     */
    @Test
    public void canGetRedirectDeflateBinding() {
        Assert.assertNotNull(this.config.httpRedirectDeflateBinding());
    }

    /**
     * Make sure can get a valid SOAP 11 binding.
     */
    @Test
    public void canGetHttpSOAP11Binding() {
        Assert.assertNotNull(this.config.httpSOAP11Binding());
    }

    /**
     * Make sure can get a valid PAOS 11 binding.
     */
    @Test
    public void canGetHttpPAOS11Binding() {
        Assert.assertNotNull(this.config.httpPAOS11Binding());
    }

    /**
     * Make sure can get a valid SAML processor with all the pertinent bindings.
     */
    @Test
    public void canGetSAMLProcessor() {
        Assert.assertNotNull(this.config.processor());
    }

    /**
     * Make sure we can get a valid SAML security processing filter chain.
     *
     * @throws Exception on any problem
     */
    @Ignore
    @Test
    public void canGetSAMLFilterChain() throws Exception {
        final FilterChainProxy filterChainProxy = this.config.samlFilter();
        Assert.assertNotNull(filterChainProxy);
        Assert.assertThat(filterChainProxy.getFilterChains().size(), Matchers.is(7));
    }

    private SAMLProperties setupForKeyManager() {
        final ResourceLoader resourceLoader = Mockito.mock(ResourceLoader.class);
        this.config.setResourceLoader(resourceLoader);
        final Resource storeFile = Mockito.mock(Resource.class);

        final SAMLProperties properties = Mockito.mock(SAMLProperties.class);
        this.config.setSamlProperties(properties);

        final String keyStorePassword = UUID.randomUUID().toString();
        final String keyStoreName = UUID.randomUUID().toString() + ".jks";
        final String defaultKeyName = UUID.randomUUID().toString();
        final String defaultKeyPassword = UUID.randomUUID().toString();

        final SAMLProperties.Keystore keyStore = Mockito.mock(SAMLProperties.Keystore.class);
        final SAMLProperties.Keystore.DefaultKey defaultKey = Mockito.mock(SAMLProperties.Keystore.DefaultKey.class);
        Mockito.when(properties.getKeystore()).thenReturn(keyStore);
        Mockito.when(keyStore.getName()).thenReturn(keyStoreName);
        Mockito.when(keyStore.getPassword()).thenReturn(keyStorePassword);
        Mockito.when(keyStore.getDefaultKey()).thenReturn(defaultKey);
        Mockito.when(defaultKey.getName()).thenReturn(defaultKeyName);
        Mockito.when(defaultKey.getPassword()).thenReturn(defaultKeyPassword);
        Mockito.when(resourceLoader.getResource(Mockito.eq("classpath:" + keyStoreName))).thenReturn(storeFile);

        return properties;
    }

    private SAMLProperties setupForMetadataGenerator() {
        final SAMLProperties properties = this.setupForKeyManager();

        final String entityId = UUID.randomUUID().toString();
        final SAMLProperties.Sp sp = Mockito.mock(SAMLProperties.Sp.class);
        Mockito.when(sp.getEntityId()).thenReturn(entityId);
        Mockito.when(properties.getSp()).thenReturn(sp);

        return properties;
    }
}
