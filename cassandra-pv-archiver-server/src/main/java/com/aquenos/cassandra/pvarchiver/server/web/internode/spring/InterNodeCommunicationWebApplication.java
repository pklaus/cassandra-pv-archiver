/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.internode.spring;

import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.web.filter.OrderedCharacterEncodingFilter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.Http403ForbiddenEntryPoint;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.aquenos.cassandra.pvarchiver.server.spring.ServerProperties;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.ConfigureCompressionTomcatConnectorCustomizer;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.CustomWebMvcConfiguration;
import com.aquenos.cassandra.pvarchiver.server.web.internode.controller.ApiController;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * Configuration class for the {@link SpringApplication} that provides the
 * inter-node communication interface to the archive server. This configuration
 * takes care of setting up an embedded Tomcat server and configuring Spring Web
 * MVC and Spring Security as well as the other components making up the web
 * application.
 * 
 * @author Sebastian Marsching
 */
@Import({ CustomWebMvcConfiguration.class })
public class InterNodeCommunicationWebApplication {

    /**
     * <p>
     * Spring Security configuration. This configuration sets up the
     * {@link AuthenticationManager} and enables the Web MVC security. Global
     * method security is not enabled because we use path-based protection.
     * </p>
     * 
     * <p>
     * This configuration turns on HTTP Basic authentication. CSRF protection is
     * not enabled because we do not have browser clients and without a session,
     * we cannot use it anyway.
     * </p>
     * 
     * @author Sebastian Marsching
     */
    @Configuration
    @EnableWebSecurity
    public static class WebSecurityConfiguration extends
            WebSecurityConfigurerAdapter {

        @Override
        protected void configure(HttpSecurity http) throws Exception {
            http.sessionManagement()
                    .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                    .and().csrf().disable().logout().disable().httpBasic()
                    .authenticationEntryPoint(new Http403ForbiddenEntryPoint())
                    .and().authorizeRequests()
                    .mvcMatchers("/inter-node/api/1.0/current-system-time")
                    .hasRole("INTER_NODE_UNPRIVILEGED").anyRequest()
                    .hasRole("INTER_NODE_PRIVILEGED");
        }

        @Override
        protected void configure(AuthenticationManagerBuilder auth)
                throws Exception {
            auth.authenticationProvider(interNodeCommunicationAuthenticationProvider());
        }

        /**
         * Creates the authentication provider for this web application. This
         * methods creates an instance of
         * {@link InterNodeCommunicationAuthenticationProvider}.
         * 
         * @return authentication provider for this web application.
         */
        @Bean
        public AuthenticationProvider interNodeCommunicationAuthenticationProvider() {
            return new InterNodeCommunicationAuthenticationProvider();
        }

    }

    /**
     * Configuration for Spring Web MVC. This configuration disables content
     * negotiation based on file extensions and makes some customizations to the
     * {@link ObjectMapper} used for converting JSON.
     * 
     * @author Sebastian Marsching
     */
    @Configuration
    public static class WebMvcConfiguration extends WebMvcConfigurerAdapter {

        @Override
        public void configureContentNegotiation(
                ContentNegotiationConfigurer configurer) {
            configurer.favorPathExtension(false);
            configurer.useJaf(false);
            configurer.replaceMediaTypes(Collections
                    .<String, MediaType> emptyMap());
        }

        @Override
        public void configurePathMatch(PathMatchConfigurer configurer) {
            configurer.setUseRegisteredSuffixPatternMatch(false);
            configurer.setUseSuffixPatternMatch(false);
        }

        @Override
        public void extendMessageConverters(
                List<HttpMessageConverter<?>> converters) {
            // Spring uses some inconvenient options when configuring Jackson's
            // ObjectMapper. Therefore, we want to change a few of these
            // options.
            Optional<HttpMessageConverter<?>> jackson2Converter = Iterables
                    .tryFind(
                            converters,
                            Predicates
                                    .instanceOf(MappingJackson2HttpMessageConverter.class));
            if (jackson2Converter.isPresent()) {
                MappingJackson2HttpMessageConverter converter = (MappingJackson2HttpMessageConverter) jackson2Converter
                        .get();
                ObjectMapper objectMapper = ((MappingJackson2HttpMessageConverter) jackson2Converter
                        .get()).getObjectMapper();
                // We want to enable FAIL_ON_NULL_FOR_PRIMITIVES so that an
                // exception is thrown if a client sends malformed input.
                objectMapper.configure(
                        DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                        true);
                // Spring sets FAIL_ON_UNKNOWN_PROPERTIES to false, but we like
                // Jackson's default (true) better.
                converter
                        .getObjectMapper()
                        .configure(
                                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                                true);
            }
        }

    }

    private ServerProperties serverProperties;

    /**
     * Sets the server properties used by this configuration. Typically, this
     * method is called by the container.
     * 
     * @param serverProperties
     *            server properties to be used by this configuration.
     */
    @Autowired
    public void setServerProperties(ServerProperties serverProperties) {
        this.serverProperties = serverProperties;
    }

    /**
     * Creates a filter that sets the character encoding of requests and
     * responses. The filter is configured to set the character encoding to
     * "UTF-8" if no character encoding has been specified explicitly.
     * 
     * @return character-encoding filter.
     */
    @Bean
    public CharacterEncodingFilter characterEncodingFilter() {
        CharacterEncodingFilter filter = new OrderedCharacterEncodingFilter();
        filter.setEncoding("UTF-8");
        return filter;
    }

    /**
     * Creates the dispatcher servlet. The dispatcher servlet handles all
     * requests and delegates them to the filters and handlers registered within
     * the {@link ApplicationContext}.
     * 
     * @return dispatcher servlet for this application.
     */
    @Bean
    public DispatcherServlet dispatcherServlet() {
        return new DispatcherServlet();
    }

    /**
     * Creates the Tomcat servlet-container factory. This factory is used by the
     * container to create the embedded Tomcat instance for this application.
     * The factory is configured to use the port returned by
     * {@link ServerProperties#getInterNodeCommunicationPort()}.
     * 
     * @return Tomcat servlet-container factory that creates the embedded Tomcat
     *         server used by this application.
     */
    @Bean
    public TomcatEmbeddedServletContainerFactory servletContainerFactory() {
        TomcatEmbeddedServletContainerFactory factory = new TomcatEmbeddedServletContainerFactory(
                serverProperties.getInterNodeCommunicationPort());
        factory.setAddress(serverProperties.getListenAddress());
        // We want to enable compression for all connectors. The easiest way to
        // do this is using a connector customizer.
        factory.addConnectorCustomizers(ConfigureCompressionTomcatConnectorCustomizer
                .enableCompression());
        return factory;
    }

    /**
     * Creates the controller that handles requests to the inter-node
     * communication API.
     * 
     * @return API controller.
     */
    @Bean
    public ApiController apiController() {
        return new ApiController();
    }

}
