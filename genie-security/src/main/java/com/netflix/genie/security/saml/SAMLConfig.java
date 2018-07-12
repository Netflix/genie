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
import com.netflix.genie.security.x509.X509UserDetailsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.velocity.app.VelocityEngine;
import org.opensaml.saml2.metadata.provider.HTTPMetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.parse.ParserPool;
import org.opensaml.xml.parse.StaticBasicParserPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.saml.SAMLAuthenticationProvider;
import org.springframework.security.saml.SAMLBootstrap;
import org.springframework.security.saml.SAMLDiscovery;
import org.springframework.security.saml.SAMLEntryPoint;
import org.springframework.security.saml.SAMLLogoutFilter;
import org.springframework.security.saml.SAMLLogoutProcessingFilter;
import org.springframework.security.saml.SAMLProcessingFilter;
import org.springframework.security.saml.SAMLWebSSOHoKProcessingFilter;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
import org.springframework.security.saml.context.SAMLContextProviderLB;
import org.springframework.security.saml.key.JKSKeyManager;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLDefaultLogger;
import org.springframework.security.saml.metadata.CachingMetadataManager;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataDisplayFilter;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.metadata.MetadataGeneratorFilter;
import org.springframework.security.saml.parser.ParserPoolHolder;
import org.springframework.security.saml.processor.HTTPArtifactBinding;
import org.springframework.security.saml.processor.HTTPPAOS11Binding;
import org.springframework.security.saml.processor.HTTPPostBinding;
import org.springframework.security.saml.processor.HTTPRedirectDeflateBinding;
import org.springframework.security.saml.processor.HTTPSOAP11Binding;
import org.springframework.security.saml.processor.SAMLBinding;
import org.springframework.security.saml.processor.SAMLProcessorImpl;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;
import org.springframework.security.saml.util.VelocityFactory;
import org.springframework.security.saml.websso.ArtifactResolutionProfile;
import org.springframework.security.saml.websso.ArtifactResolutionProfileImpl;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.SingleLogoutProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileConsumerHoKImpl;
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;
import org.springframework.security.saml.websso.WebSSOProfileECPImpl;
import org.springframework.security.saml.websso.WebSSOProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileOptions;
import org.springframework.security.web.DefaultSecurityFilterChain;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.channel.ChannelProcessingFilter;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

/**
 * Security configuration for SAML based authentication.
 * <p>
 * Modified from: https://github.com/vdenotaris/spring-boot-security-saml-sample which is basically a port of the
 * context-xml from Spring SAML example.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty(value = "genie.security.saml.enabled", havingValue = "true")
@Configuration
@Order(5)
//@EnableGlobalMethodSecurity(securedEnabled = true)
@Slf4j
public class SAMLConfig extends WebSecurityConfigurerAdapter {

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private X509UserDetailsService x509UserDetailsService;

    @Autowired
    private SAMLProperties samlProperties;

    /**
     * Initialization of OpenSAML library.
     *
     * @return the OpenSAML bootstrap object
     * @see SAMLBootstrap
     */
    @Bean
    public static SAMLBootstrap samlBootstrap() {
        return new SAMLBootstrap();
    }

    /**
     * Initialize the velocity engine.
     *
     * @return The velocity engine.
     * @see VelocityEngine
     */
    @Bean
    public VelocityEngine velocityEngine() {
        return VelocityFactory.getEngine();
    }

    /**
     * Parser pool used for the OpenSAML parsing.
     *
     * @return The parser pool
     * @see StaticBasicParserPool
     */
    @Bean(initMethod = "initialize")
    public StaticBasicParserPool parserPool() {
        return new StaticBasicParserPool();
    }

    /**
     * The holder for the parser poole.
     *
     * @return The parser pool holder
     * @see ParserPoolHolder
     */
    @Bean(name = "parserPoolHolder")
    public ParserPoolHolder parserPoolHolder() {
        return new ParserPoolHolder();
    }

    /**
     * Connection pool for the HTTP Client.
     *
     * @return a Multithreaded connection manager
     * @see MultiThreadedHttpConnectionManager
     */
    @Bean
    public MultiThreadedHttpConnectionManager multiThreadedHttpConnectionManager() {
        return new MultiThreadedHttpConnectionManager();
    }

    /**
     * The HTTP Client used to communicate with the IDP.
     *
     * @return The http client
     * @see HttpClient
     */
    @Bean
    public HttpClient httpClient() {
        return new HttpClient(multiThreadedHttpConnectionManager());
    }

    /**
     * Parses the response SAML messages.
     *
     * @param samlUserDetailsService The user details service to use
     * @return The SAML authentication provider
     * @see SAMLAuthenticationProvider
     */
    @Bean
    public SAMLAuthenticationProvider samlAuthenticationProvider(final SAMLUserDetailsService samlUserDetailsService) {
        final SAMLAuthenticationProvider samlAuthenticationProvider = new SAMLAuthenticationProvider();
        samlAuthenticationProvider.setUserDetails(samlUserDetailsService);
        samlAuthenticationProvider.setForcePrincipalAsString(false);
        return samlAuthenticationProvider;
    }

    /**
     * Provider of the SAML context.
     *
     * @param properties The SAML properties to use
     * @return the context provider implementation
     * @see SAMLContextProviderImpl
     */
    @Bean
    public SAMLContextProviderImpl contextProvider(final SAMLProperties properties) {
        if (properties.getLoadBalancer() != null) {
            log.info("Using SAMLContextProviderLB implementation of SAMLContextProvider for context provider bean.");
            final SAMLContextProviderLB lb = new SAMLContextProviderLB();
            final SAMLProperties.LoadBalancer lbProps = properties.getLoadBalancer();
            final String scheme = lbProps.getScheme();
            log.info("Setting the load balancer scheme to {}", scheme);
            lb.setScheme(scheme);
            final String serverName = lbProps.getServerName();
            log.info("Setting the load balancer server name to {}", serverName);
            lb.setServerName(serverName);
            final String contextPath = lbProps.getContextPath();
            log.info("Setting the load balancer context path to {}", contextPath);
            lb.setContextPath(contextPath);
            final int serverPort = lbProps.getServerPort();
            log.info("Setting the load balancer port to {}", serverPort);
            lb.setServerPort(serverPort);
            final boolean includeServerPort = lbProps.isIncludeServerPortInRequestURL();
            log.info("Setting whether to include the server port in the request URL to {}", includeServerPort);
            lb.setIncludeServerPortInRequestURL(includeServerPort);
            return lb;
        } else {
            log.info("Using SAMLContextProviderImpl implementation of SAMLContextProvider for context provider bean.");
            return new SAMLContextProviderImpl();
        }
    }

    /**
     * The Logger used by the SAML package.
     *
     * @return the logger
     * @see SAMLDefaultLogger
     */
    @Bean
    public SAMLDefaultLogger samlLogger() {
        return new SAMLDefaultLogger();
    }

    /**
     * SAML 2.0 WebSSO Assertion Consumer.
     *
     * @return The consumer
     * @see WebSSOProfileConsumer
     * @see WebSSOProfileConsumerImpl
     */
    @Bean
    public WebSSOProfileConsumer webSSOprofileConsumer() {
        return new WebSSOProfileConsumerImpl();
    }

    /**
     * SAML 2.0 Holder-of-Key WebSSO Assertion Consumer.
     *
     * @return The holder of key consumer implementation
     * @see WebSSOProfileConsumerHoKImpl
     */
    @Bean
    public WebSSOProfileConsumerHoKImpl hokWebSSOprofileConsumer() {
        return new WebSSOProfileConsumerHoKImpl();
    }

    /**
     * SAML 2.0 Web SSO profile.
     *
     * @return The web profile
     * @see WebSSOProfile
     * @see WebSSOProfileImpl
     */
    @Bean
    public WebSSOProfile webSSOprofile() {
        return new WebSSOProfileImpl();
    }

    /**
     * SAML 2.0 Holder-of-Key Web SSO profile.
     *
     * @return the HoK profile
     * @see WebSSOProfileConsumerHoKImpl
     */
    @Bean
    public WebSSOProfileConsumerHoKImpl hokWebSSOProfile() {
        return new WebSSOProfileConsumerHoKImpl();
    }

    /**
     * SAML 2.0 ECP profile.
     *
     * @return The ECP profile
     * @see WebSSOProfileECPImpl
     */
    @Bean
    public WebSSOProfileECPImpl ecpprofile() {
        return new WebSSOProfileECPImpl();
    }

    /**
     * The logout profile for SAML single logout.
     *
     * @return The logout profile
     * @see SingleLogoutProfile
     * @see SingleLogoutProfileImpl
     */
    @Bean
    public SingleLogoutProfile logoutProfile() {
        return new SingleLogoutProfileImpl();
    }

    /**
     * Central storage of cryptographic keys.
     *
     * @return The key manager used for everything
     * @see KeyManager
     */
    @Bean
    public KeyManager keyManager() {
        final Resource storeFile
            = this.resourceLoader.getResource("classpath:" + this.samlProperties.getKeystore().getName());
        final Map<String, String> passwords = new HashMap<>();
        passwords.put(
            this.samlProperties.getKeystore().getDefaultKey().getName(),
            this.samlProperties.getKeystore().getDefaultKey().getPassword()
        );
        return new JKSKeyManager(
            storeFile,
            this.samlProperties.getKeystore().getPassword(),
            passwords,
            this.samlProperties.getKeystore().getDefaultKey().getName()
        );
    }

    /**
     * The Web SSO profile options.
     *
     * @return The profile options for web sso
     * @see WebSSOProfileOptions
     */
    @Bean
    public WebSSOProfileOptions defaultWebSSOProfileOptions() {
        final WebSSOProfileOptions webSSOProfileOptions = new WebSSOProfileOptions();
        webSSOProfileOptions.setIncludeScoping(false);
        return webSSOProfileOptions;
    }

    /**
     * Entry point to initialize authentication, default values taken from properties file.
     *
     * @return The SAML entry point
     * @see SAMLEntryPoint
     */
    @Bean
    public SAMLEntryPoint samlEntryPoint() {
        final SAMLEntryPoint samlEntryPoint = new SAMLEntryPoint();
        samlEntryPoint.setDefaultProfileOptions(defaultWebSSOProfileOptions());
        return samlEntryPoint;
    }

    /**
     * Setup the extended metadata for the SAML request.
     *
     * @return The extended metadata
     * @see ExtendedMetadata
     */
    @Bean
    public ExtendedMetadata extendedMetadata() {
        return new ExtendedMetadata();
    }

    /**
     * Setup the IDP discovery service.
     *
     * @return The discovery service
     * @see SAMLDiscovery
     */
    @Bean
    public SAMLDiscovery samlIDPDiscovery() {
        return new SAMLDiscovery();
    }

    /**
     * Setup the extended metadata delegate for the IDP.
     *
     * @param properties The SAML properties
     * @return The sso circle of trust metadata provider configured via the url.
     * @throws MetadataProviderException On any configuration error
     * @see ExtendedMetadataDelegate
     * @see HTTPMetadataProvider
     */
    @Bean
    @Qualifier("idp-ssocircle")
    public ExtendedMetadataDelegate ssoCircleExtendedMetadataProvider(
        final SAMLProperties properties
    ) throws MetadataProviderException {
        // Create a daemon timer for updating the IDP metadata from the server
        final Timer backgroundTaskTimer = new Timer(true);
        final HTTPMetadataProvider httpMetadataProvider = new HTTPMetadataProvider(
            backgroundTaskTimer,
            httpClient(),
            properties.getIdp().getServiceProviderMetadataURL()
        );
        httpMetadataProvider.setParserPool(parserPool());
        final ExtendedMetadataDelegate extendedMetadataDelegate
            = new ExtendedMetadataDelegate(httpMetadataProvider, extendedMetadata());
        extendedMetadataDelegate.setMetadataTrustCheck(true);
        extendedMetadataDelegate.setMetadataRequireSignature(false);
        return extendedMetadataDelegate;
    }

    /**
     * Get the metadata manager for the IDP metadata. This version caches locally and refreshes periodically.
     *
     * @param ssoCircleExtendedMetadataProvider The extended metadata delegate
     * @return The metadata manager
     * @throws MetadataProviderException on any configuration error
     * @see CachingMetadataManager
     */
    @Bean
    @Qualifier("metadata")
    public CachingMetadataManager metadata(
        final ExtendedMetadataDelegate ssoCircleExtendedMetadataProvider
    ) throws MetadataProviderException {
        return new CachingMetadataManager(Lists.newArrayList(ssoCircleExtendedMetadataProvider));
    }

    /**
     * Generates default SP metadata if none is set.
     *
     * @return The metadata generator filter
     * @see MetadataGenerator
     */
    @Bean
    public MetadataGenerator metadataGenerator() {
        final MetadataGenerator metadataGenerator = new MetadataGenerator();
        metadataGenerator.setEntityId(this.samlProperties.getSp().getEntityId());
        metadataGenerator.setExtendedMetadata(extendedMetadata());
        metadataGenerator.setIncludeDiscoveryExtension(false);
        metadataGenerator.setKeyManager(keyManager());
        if (this.samlProperties.getSp().getEntityBaseURL() != null) {
            metadataGenerator.setEntityBaseURL(this.samlProperties.getSp().getEntityBaseURL());
        }
        return metadataGenerator;
    }

    /**
     * The filter is waiting for connections on URL suffixed with filterSuffix and presents SP metadata there.
     *
     * @return the filter that will display the SP information
     * @see MetadataDisplayFilter
     */
    @Bean
    public MetadataDisplayFilter metadataDisplayFilter() {
        return new MetadataDisplayFilter();
    }

    /**
     * Handler deciding where to redirect user after successful login.
     *
     * @return The login success handler
     * @see SavedRequestAwareAuthenticationSuccessHandler
     */
    @Bean
    public SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler() {
        return new SavedRequestAwareAuthenticationSuccessHandler();
    }

    /**
     * Handler deciding where to redirect user after failed login.
     *
     * @return Authentication failure handler
     * @see SimpleUrlAuthenticationFailureHandler
     */
    @Bean
    public SimpleUrlAuthenticationFailureHandler authenticationFailureHandler() {
        final SimpleUrlAuthenticationFailureHandler failureHandler = new SimpleUrlAuthenticationFailureHandler();
        failureHandler.setUseForward(true);
        failureHandler.setDefaultFailureUrl("/error");
        return failureHandler;
    }

    /**
     * Filter to process holder of key sso requests.
     *
     * @return The filter
     * @throws Exception For any configuration error
     * @see SAMLWebSSOHoKProcessingFilter
     */
    @Bean
    public SAMLWebSSOHoKProcessingFilter samlWebSSOHoKProcessingFilter() throws Exception {
        final SAMLWebSSOHoKProcessingFilter samlWebSSOHoKProcessingFilter = new SAMLWebSSOHoKProcessingFilter();
        samlWebSSOHoKProcessingFilter.setAuthenticationSuccessHandler(successRedirectHandler());
        samlWebSSOHoKProcessingFilter.setAuthenticationManager(authenticationManager());
        samlWebSSOHoKProcessingFilter.setAuthenticationFailureHandler(authenticationFailureHandler());
        return samlWebSSOHoKProcessingFilter;
    }

    /**
     * Processing filter for WebSSO profile messages.
     *
     * @return The SAML processing filter
     * @throws Exception on any configuration error
     * @see SAMLProcessingFilter
     */
    @Bean
    public SAMLProcessingFilter samlWebSSOProcessingFilter() throws Exception {
        final SAMLProcessingFilter samlWebSSOProcessingFilter = new SAMLProcessingFilter();
        samlWebSSOProcessingFilter.setAuthenticationManager(authenticationManager());
        samlWebSSOProcessingFilter.setAuthenticationSuccessHandler(successRedirectHandler());
        samlWebSSOProcessingFilter.setAuthenticationFailureHandler(authenticationFailureHandler());
        return samlWebSSOProcessingFilter;
    }

    /**
     * The metadata generator filter which generates metadata for the SP if non is pre-configured.
     *
     * @return The metadata generator filter
     */
    @Bean
    public MetadataGeneratorFilter metadataGeneratorFilter() {
        return new MetadataGeneratorFilter(metadataGenerator());
    }

    /**
     * Handler for successful logout.
     *
     * @return the logout location to redirect the user to after they logout
     * @see SimpleUrlLogoutSuccessHandler
     */
    @Bean
    public SimpleUrlLogoutSuccessHandler successLogoutHandler() {
        final SimpleUrlLogoutSuccessHandler successLogoutHandler = new SimpleUrlLogoutSuccessHandler();
        //TODO: Change this location as it looks like could send into a circular loop
        successLogoutHandler.setDefaultTargetUrl("/");
        return successLogoutHandler;
    }

    /**
     * Logout handler terminating local session.
     *
     * @return The security context logout handler
     * @see SecurityContextLogoutHandler
     */
    @Bean
    public SecurityContextLogoutHandler logoutHandler() {
        final SecurityContextLogoutHandler logoutHandler = new SecurityContextLogoutHandler();
        logoutHandler.setInvalidateHttpSession(true);
        logoutHandler.setClearAuthentication(true);
        return logoutHandler;
    }

    /**
     * Filter to handle logout requests.
     *
     * @return The filter
     * @see SAMLLogoutProcessingFilter
     */
    @Bean
    public SAMLLogoutProcessingFilter samlLogoutProcessingFilter() {
        return new SAMLLogoutProcessingFilter(successLogoutHandler(), logoutHandler());
    }

    /**
     * Overrides default logout processing filter with the one processing SAML messages.
     *
     * @return The logout filter
     * @see SAMLLogoutFilter
     */
    @Bean
    public SAMLLogoutFilter samlLogoutFilter() {
        return new SAMLLogoutFilter(
            successLogoutHandler(),
            new LogoutHandler[]{logoutHandler()},
            new LogoutHandler[]{logoutHandler()}
        );
    }

    // Bindings
    private ArtifactResolutionProfile artifactResolutionProfile() {
        final ArtifactResolutionProfileImpl artifactResolutionProfile = new ArtifactResolutionProfileImpl(httpClient());
        artifactResolutionProfile.setProcessor(new SAMLProcessorImpl(soapBinding()));
        return artifactResolutionProfile;
    }

    /**
     * HTTP Artifact binding.
     *
     * @param parserPool     The parser pool to use
     * @param velocityEngine The velocity engine to use
     * @return The artifact binding
     * @see HTTPArtifactBinding
     */
    @Bean
    public HTTPArtifactBinding artifactBinding(final ParserPool parserPool, final VelocityEngine velocityEngine) {
        return new HTTPArtifactBinding(parserPool, velocityEngine, artifactResolutionProfile());
    }

    /**
     * A SOAP binding to use.
     *
     * @return a SOAP binding
     * @see HTTPSOAP11Binding
     */
    @Bean
    public HTTPSOAP11Binding soapBinding() {
        return new HTTPSOAP11Binding(parserPool());
    }

    /**
     * A HTTP POST binding to use.
     *
     * @return The post binding
     * @see HTTPPostBinding
     */
    @Bean
    public HTTPPostBinding httpPostBinding() {
        return new HTTPPostBinding(parserPool(), velocityEngine());
    }

    /**
     * A HTTP redirect binding to use.
     *
     * @return The redirect binding
     * @see HTTPRedirectDeflateBinding
     */
    @Bean
    public HTTPRedirectDeflateBinding httpRedirectDeflateBinding() {
        return new HTTPRedirectDeflateBinding(parserPool());
    }

    /**
     * A SOAP binding to use.
     *
     * @return a SOAP binding
     * @see HTTPSOAP11Binding
     */
    @Bean
    public HTTPSOAP11Binding httpSOAP11Binding() {
        return new HTTPSOAP11Binding(parserPool());
    }

    /**
     * A PAOS binding to use.
     *
     * @return The PAOS binding
     * @see HTTPPAOS11Binding
     */
    @Bean
    public HTTPPAOS11Binding httpPAOS11Binding() {
        return new HTTPPAOS11Binding(parserPool());
    }

    /**
     * The SAML processor that includes bindings for various communication protocols with the IDP.
     *
     * @return The saml processor
     * @see SAMLProcessorImpl
     */
    @Bean
    public SAMLProcessorImpl processor() {
        final List<SAMLBinding> bindings = Lists.newArrayList(
            httpRedirectDeflateBinding(),
            httpPostBinding(),
            artifactBinding(parserPool(), velocityEngine()),
            httpSOAP11Binding(),
            httpPAOS11Binding()
        );
        return new SAMLProcessorImpl(bindings);
    }

    /**
     * Define the security filter chain in order to support SSO Auth by using SAML 2.0.
     *
     * @return Filter chain proxy
     * @throws Exception on any configuration problem
     * @see FilterChainProxy
     */
    @Bean
    public FilterChainProxy samlFilter() throws Exception {
        final List<SecurityFilterChain> chains = new ArrayList<>();
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/login/**"),
                samlEntryPoint()
            )
        );
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/logout/**"),
                samlLogoutFilter()
            )
        );
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/metadata/**"),
                metadataDisplayFilter()
            )
        );
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/SSO/**"),
                samlWebSSOProcessingFilter()
            )
        );
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/SSOHoK/**"),
                samlWebSSOHoKProcessingFilter()
            )
        );
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/SingleLogout/**"),
                samlLogoutProcessingFilter()
            )
        );
        chains.add(
            new DefaultSecurityFilterChain(
                new AntPathRequestMatcher("/saml/discovery/**"), samlIDPDiscovery()
            )
        );
        return new FilterChainProxy(chains);
    }

    /**
     * Defines the web based security configuration.
     *
     * @param http It allows configuring web based security for specific http requests.
     * @throws Exception on any error
     */
    @Override
    protected void configure(final HttpSecurity http) throws Exception {
        // @formatter:off
        http
            .httpBasic()
            .authenticationEntryPoint(samlEntryPoint());
        http
            .csrf()
            .disable();
        http
            .addFilterBefore(metadataGeneratorFilter(), ChannelProcessingFilter.class)
            .addFilterAfter(samlFilter(), BasicAuthenticationFilter.class);
        http
            .antMatcher("/**")
                .authorizeRequests()
                    .requestMatchers(EndpointRequest.toAnyEndpoint()).permitAll()
                    .antMatchers("/api/**").permitAll()
                    .antMatchers("/error").permitAll()
                    .antMatchers("/saml/**").permitAll()
                    .anyRequest().authenticated()
            .and()
                .x509().authenticationUserDetailsService(this.x509UserDetailsService);
        http
            .logout()
            .logoutSuccessUrl("/");
        // @formatter:on
    }

    void setSamlProperties(final SAMLProperties samlProperties) {
        this.samlProperties = samlProperties;
    }

    void setResourceLoader(final ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }
}
