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
package com.netflix.genie;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;

import javax.validation.Validator;

/**
 * Main Genie Spring Configuration class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@SpringBootApplication
public class GenieWeb extends WebMvcAutoConfiguration.WebMvcAutoConfigurationAdapter {

    /**
     * Spring Boot Main.
     *
     * @param args Program arguments
     * @throws Exception For any failure during program execution
     */
    public static void main(final String[] args) throws Exception {
        SpringApplication.run(GenieWeb.class, args);
    }

    /**
     * Setup bean validation.
     *
     * @return The bean validator
     */
    @Bean
    public Validator localValidatorFactoryBean() {
        return new LocalValidatorFactoryBean();
    }

    /**
     * Setup method parameter bean validation.
     *
     * @return The method validation processor
     */
    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

//    /**
//     * blah.
//     *
//     * @return blah.
//     */
//    @Bean
//    public EmbeddedServletContainerFactory servletContainer() {
//        final TomcatEmbeddedServletContainerFactory tomcat = new TomcatEmbeddedServletContainerFactory();
//        tomcat.setRegisterDefaultServlet(true);
//        return tomcat;
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public void configureDefaultServletHandling(final DefaultServletHandlerConfigurer configurer) {
//        configurer.enable();
//    }
//
//    /**
//     * blah.
//     *
//     * @return blah
//     */
//    @Bean
//    public EmbeddedServletContainerFactory servletContainerFactory() {
//        return new TomcatEmbeddedServletContainerFactory() {
//            @Override
//            protected TomcatEmbeddedServletContainer getTomcatEmbeddedServletContainer(final Tomcat tomcat) {
//                final DefaultServlet servlet = new DefaultServlet();
//                final Wrapper wrapper = tomcat.addServlet("/genie-jobs/", "genieJobs", servlet);
//                wrapper.addInitParameter("listings", "true");
//                tomcat.addContext("/genie-jobs/*", "/Users/tgianos/Projects/tmp/genie/");
//                return super.getTomcatEmbeddedServletContainer(tomcat);
//            }
//        };
//    }
//
//    /**
//     * blah.
//     *
//     * @return blah
//     */
//    @Bean
//    public EmbeddedServletContainerFactory servletContainerFactory() {
//        return new TomcatEmbeddedServletContainerFactory() {
//
//            /**
//             * {@inheritDoc}
//             */
//            @Override
//            protected TomcatEmbeddedServletContainer getTomcatEmbeddedServletContainer(final Tomcat tomcat) {
//                try {
////                    tomcat.addWebapp("/genie-jobs", "/Users/tgianos/Projects/tmp/genie/");
////                    final Wrapper wrapper = tomcat.addServlet("/genie-jobs/*", "genieJobs", new DefaultServlet());
////                    wrapper.addInitParameter("listings", "true");
//                    final Context context = tomcat.addWebapp("/genie-jobs", "/Users/tgianos/Projects/tmp/genie/");
////                    context.getServletContext().setInitParameter("listings", "true");
////                    LOG.info("listings = {}", context.getServletContext().getInitParameter("listings"));
////                    final WebappLoader loader = new WebappLoader(Thread.currentThread().getContextClassLoader());
////                    context.setLoader(loader);
//                } catch (final ServletException ex) {
//                    throw new IllegalStateException("Failed to add webapp", ex);
//                }
//                return super.getTomcatEmbeddedServletContainer(tomcat);
//            }
//
//        };
//    }
//
//    /**
//     * blah.
//     *
//     * @return blah
//     */
//    @Bean
//    public ServletRegistrationBean servletRegistrationBean() {
//        final DefaultServlet servlet = new DefaultServlet();
//        final ServletRegistrationBean bean = new ServletRegistrationBean(servlet, "/genie-jobs/*");
//        bean.addInitParameter("listings", "true");
//        bean.setLoadOnStartup(1);
//        return bean;
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
        super.addResourceHandlers(registry);

        final String myExternalFilePath = "file:///Users/tgianos/Projects/tmp/genie/";

        registry
            .addResourceHandler("/genie-jobs/**")
            .setCachePeriod(0)
            .addResourceLocations(myExternalFilePath);
    }
}
