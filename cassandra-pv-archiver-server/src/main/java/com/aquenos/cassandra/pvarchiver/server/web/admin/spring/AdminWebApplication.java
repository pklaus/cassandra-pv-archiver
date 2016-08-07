/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.spring;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.AbstractProtocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.boot.web.filter.OrderedCharacterEncodingFilter;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.cache.guava.GuavaCache;
import org.springframework.context.ApplicationContext;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;
import org.springframework.security.core.userdetails.UserCache;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.cache.SpringCacheBasedUserCache;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.DefaultRedirectStrategy;
import org.springframework.security.web.authentication.Http403ForbiddenEntryPoint;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.util.UrlUtils;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.LocaleResolver;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewResolverRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.thymeleaf.spring4.view.ThymeleafViewResolver;

import com.aquenos.cassandra.pvarchiver.server.spring.DevelopmentModeProperties;
import com.aquenos.cassandra.pvarchiver.server.spring.ServerProperties;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.ApiController;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.IndexRedirectController;
import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.UiController;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.AcceptHeaderLocaleResolver;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.ConfigureCompressionTomcatConnectorCustomizer;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.CustomWebMvcConfiguration;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.MimeMappingsExtensions;
import com.aquenos.cassandra.pvarchiver.server.web.common.spring.ThymeleafConfiguration;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Configuration class for the {@link SpringApplication} that provides the
 * administrative interface to the archive server. This configuration takes care
 * of setting up an embedded Tomcat server and configuring Spring Web MVC and
 * Spring Security as well as the other components making up the web
 * application.
 * 
 * @author Sebastian Marsching
 */
@Import({ CustomWebMvcConfiguration.class })
public class AdminWebApplication {

    /**
     * Thymeleaf configuration using a custom path for the template prefix. This
     * application stores its templates in
     * <code>classpath:/META-INF/templates/admin/</code>.
     * 
     * @author Sebastian Marsching
     */
    @Configuration
    public static class CustomThymeleafConfiguration extends
            ThymeleafConfiguration {

        @Override
        protected String getTemplatePrefix() {
            return "classpath:/META-INF/templates/admin/";
        }

    }

    /**
     * Spring Security configuration. This configuration sets up the
     * {@link AuthenticationManager} and enabled the global method security and
     * Web MVC security. The actual configuration of Web MVC security is handled
     * by {@link AdminApiHttpSecurityConfiguration} and
     * {@link AdminUiHttpSecurityConfiguration}.
     * 
     * @author Sebastian Marsching
     */
    @Configuration
    @EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, jsr250Enabled = true, proxyTargetClass = true)
    @EnableWebSecurity
    public static class WebSecurityConfiguration {

        /**
         * Security configuration for the <code>/admin/ui/</code> URI. This
         * configuration turns on form-based login and enables CSRF protection.
         * However, it does not impose any access restrictions. Access
         * restrictions are enforced through annotations at the controller
         * level.
         * 
         * @author Sebastian Marsching
         */
        @Order(1)
        @Configuration
        public static class AdminUiHttpSecurityConfiguration extends
                WebSecurityConfigurerAdapter {

            @Override
            protected void configure(HttpSecurity http) throws Exception {
                SavedRequestAwareAuthenticationSuccessHandler authenticationSuccessHandler = new SavedRequestAwareAuthenticationSuccessHandler();
                DefaultRedirectStrategy redirectStrategy = new DefaultRedirectStrategy() {

                    @Override
                    public void sendRedirect(HttpServletRequest request,
                            HttpServletResponse response, String url)
                            throws IOException {
                        // For security reasons, we do not want to allow
                        // absolute URLs. However, if the absolute URL has the
                        // same host, using it should be saved.
                        // Unfortunately, the
                        // SavedRequestAwareAuthenticationSuccessHandler uses
                        // the absolute URL stored in the saved request, so we
                        // cannot disable absolute URLs completely.
                        // On the other hand, recent versions of the
                        // AbstractAuthenticationTargetUrlRequestHandler (the
                        // base class of
                        // SavedRequestAwareAuthenticationSuccessHandler) do not
                        // use any external parameters for the target URL any
                        // longer unless explicitly enabled. For this reason,
                        // even allowing arbitrary absolute URLs should now be
                        // safe.
                        if (UrlUtils.isAbsoluteUrl(url)) {
                            String fullRequestUrl = UrlUtils
                                    .buildFullRequestUrl(request);
                            try {
                                if (!new URI(url).getHost().equals(
                                        new URI(fullRequestUrl).getHost())) {
                                    url = "/admin/ui";
                                }
                            } catch (URISyntaxException e) {
                                url = "/admin/ui";
                            }
                        }
                        super.sendRedirect(request, response, url);
                    }

                };
                authenticationSuccessHandler
                        .setRedirectStrategy(redirectStrategy);
                authenticationSuccessHandler.setDefaultTargetUrl("/admin/ui/");
                // It is safe to use the targetUrl parameter for two reasons:
                // 1) We only accept POST requests to the login handler, thus a
                // malicious link cannot cause a redirect. Only a malicious
                // form could trigger such a redirect and we already have more
                // severe problems if a user enters the credentials into a
                // malicious form.
                // 2) We use a redirect strategy that does not allow absolute
                // URLs.
                authenticationSuccessHandler.setTargetUrlParameter("targetUrl");
                http.antMatcher("/admin/ui/**").csrf().and().formLogin()
                        .loginPage("/admin/ui/account/sign-in")
                        .successHandler(authenticationSuccessHandler).and()
                        .logout().logoutUrl("/admin/ui/account/sign-out");
            }
        }

        /**
         * Spring security configuration for the <code>/admin/api</code> URI.
         * This configuration turns on HTTP Basic authentication. CSRF
         * protection is only enabled if there is an HTTP session (and thus the
         * user might be logged in using form-based authentication). The HTTP
         * Basic authentication is only used by web-service clients (not by
         * browsers), thus CSRF protection is not needed in this case (and
         * actually not possible because there is no session).
         * 
         * @author Sebastian Marsching
         */
        @Order(2)
        @Configuration
        public static class AdminApiHttpSecurityConfiguration extends
                WebSecurityConfigurerAdapter {

            @Override
            protected void configure(HttpSecurity http) throws Exception {
                RequestMatcher requireCsrfProtectionMatcher = new RequestMatcher() {

                    private final Pattern requestMethodPattern = Pattern
                            .compile("^(GET|HEAD|TRACE|OPTIONS)$");

                    @Override
                    public boolean matches(HttpServletRequest request) {
                        // We only want to check for a CSRF header if this is
                        // not a get request and we do have a session. We
                        // disable session creation by Spring security so that
                        // a session should only be available when it has been
                        // created by accessing the UI. In this case, the client
                        // is a browser and the CSRF header should be available
                        // in order to protect from CSRF attacks. Non-browser
                        // clients will use HTTP basic authentication and thus
                        // there is no session. For those clients, we do not
                        // need (and without a session actually cannot have)
                        // CSRF protection.
                        // Like the standard CSRF request matcher, we do not
                        // enforce CSRF for GET, HEAD, TRACE, and OPTIONS
                        // requests, because those requests typically are safe
                        // regarding CSRF and can only get a CSRF token through
                        // a request header, which might not be supported by all
                        // clients.
                        return !requestMethodPattern.matcher(
                                request.getMethod()).matches()
                                && request.getSession(false) != null;
                    }

                };
                http.antMatcher("/admin/api/**")
                        .sessionManagement()
                        .sessionCreationPolicy(SessionCreationPolicy.NEVER)
                        .and()
                        .csrf()
                        .requireCsrfProtectionMatcher(
                                requireCsrfProtectionMatcher)
                        .and()
                        .httpBasic()
                        .authenticationEntryPoint(
                                new Http403ForbiddenEntryPoint());
            }
        }

        private static final GrantedAuthority ROLE_AUTHENTICATED_USER_AUTHORITY = new SimpleGrantedAuthority(
                "ROLE_AUTHENTICATED_USER");
        private static final Predicate<GrantedAuthority> IS_ROLE_AUTHENTICATED_USER_AUTHORITY = new Predicate<GrantedAuthority>() {
            @Override
            public boolean apply(GrantedAuthority input) {
                return (input != null && input.getAuthority() != null && input
                        .getAuthority().equals(
                                ROLE_AUTHENTICATED_USER_AUTHORITY
                                        .getAuthority()));
            }
        };

        /**
         * Creates the authentication provider used by this web application. The
         * authentication provider is configured to use the specified user
         * details service and the specified password encoder. It is also
         * configured to use a {@link UserCache} in order to reduce the latency
         * when authentication requests are very frequent (e.g. when HTTP Basic
         * Authentication is being used). This method is typically invoked by
         * the Spring container, passing references to the dependencies that
         * point to the respective beans in the parent context.
         * 
         * @param passwordEncoder
         *            password encoder to be used for authentication. Typically,
         *            this parameter is automatically filled with the correct
         *            instance by the Spring container.
         * @param userDetailsService
         *            user details service to be used for authentication.
         *            Typically, this parameter is automatically filled with the
         *            correct instance by the Spring container.
         * @return authentication provider for this web application.
         * @throws Exception
         *             if a problem prevents the creation of the authentication
         *             provider.
         */
        @Bean
        public AuthenticationProvider authenticationProvider(
                PasswordEncoder passwordEncoder,
                UserDetailsService userDetailsService) throws Exception {
            DaoAuthenticationProvider authenticationProvider = new DaoAuthenticationProvider();
            // We want to make sure that the authorities for authenticated users
            // always contain the "ROLE_AUTHENTICATED_USER" authority. This way,
            // we can easily restrict resources to all authenticated users
            // without having to care which different roles they might have.
            authenticationProvider
                    .setAuthoritiesMapper(new GrantedAuthoritiesMapper() {
                        @Override
                        public Collection<? extends GrantedAuthority> mapAuthorities(
                                Collection<? extends GrantedAuthority> authorities) {
                            if (Iterables.any(authorities,
                                    IS_ROLE_AUTHENTICATED_USER_AUTHORITY)) {
                                return authorities;
                            } else {
                                return ImmutableSet
                                        .<GrantedAuthority> builder()
                                        .addAll(authorities)
                                        .add(ROLE_AUTHENTICATED_USER_AUTHORITY)
                                        .build();
                            }
                        }
                    });
            authenticationProvider.setPasswordEncoder(passwordEncoder);
            // We have to use a string as the principal. If we used the actual
            // UserDetails object, the hashed password would be erased after
            // authentication, thus undermining the purpose of caching it.
            // This happens because the AuthenticationManager calls
            // eraseCredentials on the Authentication object and
            // AbstractAuthenticationToken.eraseCredentials calls
            // eraseCredentials on all its components, including the principal.
            // The alternative would be to use our own AuthenticationProvider
            // which returns a special version of
            // UsernamePasswordAuthenticationToken that does not modify the
            // principal, but this would be more complicated.
            authenticationProvider.setForcePrincipalAsString(true);
            // We keep user details in the cache for up to 10 seconds. When a
            // user is modified (e.g. the password is changed), the old object
            // might still be used for this time. When the authentication fails,
            // the cache is always bypassed, so there is no problem if the user
            // tries to login with the new password before the old entry has
            // been evicted from the cache. However, we do not want to keep
            // cached entries around for too long because the user will still be
            // able to login with the older password or a user that has been
            // removed might still be able to login for this period. In addition
            // to that, changes in the set of authorities may not become visible
            // before this period has passed.
            com.google.common.cache.Cache<Object, Object> cache = CacheBuilder
                    .newBuilder().concurrencyLevel(16)
                    .expireAfterWrite(10L, TimeUnit.SECONDS)
                    .initialCapacity(64).build();
            UserCache userCache = new SpringCacheBasedUserCache(new GuavaCache(
                    "archiveUserDetailsManagerCache", cache));
            authenticationProvider.setUserCache(userCache);
            authenticationProvider.setUserDetailsService(userDetailsService);
            return authenticationProvider;
        }

        /**
         * Configures the {@link AuthenticationManager}. This method is invoked
         * by the container when the {@link AuthenticationManager} is created.
         * This is safe, because this class is annotated with
         * {@link EnableGlobalMethodSecurity} and {@link EnableWebSecurity}.
         * 
         * @param auth
         *            the builder that is used for configuring the
         *            {@link AuthenticationManager}.
         * @throws Exception
         *             if the configuration of the authentication manager fails.
         */
        @Autowired
        public void configureAuthenticationManager(
                AuthenticationManagerBuilder auth) throws Exception {
            // Passing null for the two parameters is not elegant, but works:
            // Spring dynamically redirects the method call so that the
            // previously created singleton bean is returned and the parameters
            // are ignored. Instead, the two dependencies are automatically
            // injected using instances from the parent context.
            // In an earlier version, we had to @Autowired setter methods for
            // injecting the dependencies into this configuration class.
            // However, this did not work reliably. For some reason, the
            // authentication provider was sometimes instantiated before the
            // dependencies had been injected, resulting in an exception. This
            // solution, in contrast, seems to work reliably.
            auth.authenticationProvider(authenticationProvider(null, null));
        }

    }

    /**
     * Configuration for Spring Web MVC. This configuration adds the
     * {@link ThymeleafViewResolver} created by
     * {@link CustomThymeleafConfiguration} and an
     * {@link InternalResourceViewResolver} to the list of view resolvers and
     * configures the paths for static resources. It also disables content
     * negotiation based on file extensions. Besides, it makes some
     * customizations to the {@link ObjectMapper} used for converting JSON.
     * 
     * @author Sebastian Marsching
     */
    @Configuration
    public static class WebMvcConfiguration extends WebMvcConfigurerAdapter {

        private DevelopmentModeProperties developmentModeProperties;
        private ThymeleafViewResolver thymeleafViewResolver;

        @Override
        public void addResourceHandlers(ResourceHandlerRegistry registry) {
            // WebJARs use version numbers in the path, therefore we can safely
            // use a very long cache period.
            registry.addResourceHandler("/webjars/**")
                    .addResourceLocations(
                            "classpath:/META-INF/resources/webjars/")
                    .setCachePeriod(31536000);
            // In development mode, we want to disable caching. In production
            // mode, we allow to cache static resources for one hour. This means
            // that after an upgrade, new versions of resources are picked up in
            // a reasonable amount of time, but still saves a lot of requests.
            int cachePeriod;
            if (developmentModeProperties.isEnabled()) {
                cachePeriod = 0;
            } else {
                cachePeriod = 3600;
            }
            registry.addResourceHandler("/admin/**")
                    .addResourceLocations(
                            "classpath:/META-INF/resources/admin/")
                    .setCachePeriod(cachePeriod);
        }

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
        public void configureViewResolvers(ViewResolverRegistry registry) {
            // We prefer HTTP 1.1 (status code 303) redirects. The semantics are
            // clearer than for the HTTP 1.0 (status code 302) redirects. We do
            // not expect any HTTP 1.0 clients anyway.
            thymeleafViewResolver.setRedirectHttp10Compatible(false);
            registry.viewResolver(thymeleafViewResolver);
            InternalResourceViewResolver internalResourceViewResolver = new InternalResourceViewResolver();
            internalResourceViewResolver.setRedirectHttp10Compatible(false);
            registry.viewResolver(internalResourceViewResolver);
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

        /**
         * Sets the development-mode properties used by this configuration.
         * Typically, this method is called by the container.
         * 
         * @param developmentModeProperties
         *            development-mode properties to be used by this
         *            configuration.
         */
        @Autowired
        public void setDevelopmentModeProperties(
                DevelopmentModeProperties developmentModeProperties) {
            this.developmentModeProperties = developmentModeProperties;
        }

        /**
         * Sets the Thymeleaf view resolver to be used by this configuration and
         * thus Spring Web MVC. Typically, this method is called by the
         * container.
         * 
         * @param thymeleafViewResolver
         *            Thymeleaf view resolver to be used by this configuration.
         */
        @Autowired
        public void setThymeleafViewResolver(
                ThymeleafViewResolver thymeleafViewResolver) {
            this.thymeleafViewResolver = thymeleafViewResolver;
        }

    }

    private DevelopmentModeProperties developmentModeProperties;
    private ServerProperties serverProperties;

    /**
     * Sets the development-mode properties used by this configuration.
     * Typically, this method is called by the container.
     * 
     * @param developmentModeProperties
     *            development-mode properties to be used by this configuration.
     */
    @Autowired
    public void setDevelopmentModeProperties(
            DevelopmentModeProperties developmentModeProperties) {
        this.developmentModeProperties = developmentModeProperties;
    }

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
     * Creates multipart configuration that shall be used by the servlet
     * container. The multipart configuration enables the upload of files
     * (through multipart request bodies), limiting the maximum file and request
     * size to 100 MB.
     * 
     * @return multipart configuration that shall be used by the servlet
     *         container.
     */
    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        factory.setFileSizeThreshold("1MB");
        factory.setMaxFileSize("100MB");
        factory.setMaxRequestSize("100MB");
        return factory.createMultipartConfig();
    }

    /**
     * Creates the multipart resolver. As we use Servlet 3.0 multipart support,
     * this resolver simply delegates to the Servlet API.
     * 
     * @return multipart resolver used by this application.
     */
    @Bean(name = DispatcherServlet.MULTIPART_RESOLVER_BEAN_NAME)
    public StandardServletMultipartResolver multipartResolver() {
        return new StandardServletMultipartResolver();
    }

    /**
     * Creates the Tomcat servlet-container factory. This factory is used by the
     * container to create the embedded Tomcat instance for this application.
     * The factory is configured to use the port returned by
     * {@link ServerProperties#getAdminPort()}.
     * 
     * @return Tomcat servlet-container factory that creates the embedded Tomcat
     *         server used by this application.
     */
    @Bean
    public TomcatEmbeddedServletContainerFactory servletContainerFactory() {
        TomcatEmbeddedServletContainerFactory factory = new TomcatEmbeddedServletContainerFactory(
                serverProperties.getAdminPort());
        factory.setAddress(serverProperties.getListenAddress());
        // If the server does listen on a non-loop-back address (it should by
        // default), we also want to make it listen on the loop-back address.
        if (!serverProperties.getListenAddress().isLoopbackAddress()) {
            // In addition to the specified address, we want to listen on the
            // loop-back interface.
            Connector connector = new Connector(
                    TomcatEmbeddedServletContainerFactory.DEFAULT_PROTOCOL);
            connector.setPort(factory.getPort());
            // We can only set the listen address if the protocol handler is
            // derived from AbstractProtocol. If it is not, we do not add the
            // connector because there is no sense in adding an additional
            // connector if we cannot set the listen address.
            if (connector.getProtocolHandler() instanceof AbstractProtocol<?>) {
                AbstractProtocol<?> protocolHandler = (AbstractProtocol<?>) connector
                        .getProtocolHandler();
                try {
                    protocolHandler.setAddress(InetAddress.getByName(null));
                } catch (UnknownHostException e) {
                    // This should not happen for a null argument.
                    throw new RuntimeException(
                            "Could not determine loop-back address.", e);
                }
                // Except for the listen address, we want to configure the
                // connector completely the same way as Spring does it.
                if (factory.getUriEncoding() != null) {
                    connector.setURIEncoding(factory.getUriEncoding().name());
                }
                connector.setProperty("bindOnInit", "false");
                factory.addAdditionalTomcatConnectors(connector);
            }
        }
        // We want to enable compression for all connectors. The easiest way to
        // do this is using a connector customizer.
        factory.addConnectorCustomizers(ConfigureCompressionTomcatConnectorCustomizer
                .enableCompression());
        // The default configuration does not include mapping for a few file
        // types that we use, so we add them.
        factory.setMimeMappings(MimeMappingsExtensions
                .extendWithFontTypes(factory.getMimeMappings()));
        return factory;
    }

    /**
     * Creates the locale resolver for this application. We use the
     * {@link AcceptHeaderLocaleResolver}, limiting the selected locale to a set
     * of supported locales (currently only <code>en_US</code>). This ensures
     * that the same locale is used consistently instead of using the default
     * messages (English) but the user's locale (e.g. German) for formatting
     * numbers.
     * 
     * @return locale resolver for this application.
     */
    @Bean
    public LocaleResolver localeResolver() {
        AcceptHeaderLocaleResolver localeResolver = new AcceptHeaderLocaleResolver();
        localeResolver.setDefaultLocale(Locale.US);
        localeResolver.setSupportedLocales(ImmutableSet.of(Locale.US));
        return localeResolver;
    }

    /**
     * Creates the message source used by this application. The message source
     * is a {@link ReloadableResourceBundleMessageSource} that reads messages
     * from <code>classpath:/META-INF/messages/admin/messages.properties</code>
     * (and the respective variants for different locales). The properties files
     * are expected to use the UTF-8 encoding. When running in development mode,
     * messages are not cached (however, they might be cached by the class
     * loader). In production mode, messages are cached for an indefinite amount
     * of time.
     * 
     * @return message source for this application.
     */
    @Bean
    public MessageSource messageSource() {
        ReloadableResourceBundleMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource
                .setBasename("classpath:/META-INF/messages/admin/messages");
        messageSource.setDefaultEncoding("UTF-8");
        messageSource.setFallbackToSystemLocale(false);
        if (developmentModeProperties.isEnabled()) {
            messageSource.setCacheSeconds(0);
        } else {
            messageSource.setCacheSeconds(-1);
        }
        return messageSource;
    }

    /**
     * Creates the controller that handles redirects for certain root paths.
     * 
     * @return index redirect controller.
     */
    @Bean
    public IndexRedirectController indexRedirectController() {
        return new IndexRedirectController();
    }

    /**
     * Creates the controller that handles requests to the administrative API.
     * 
     * @return API controller.
     */
    @Bean
    public ApiController apiController() {
        return new ApiController();
    }

    /**
     * Creates the controller that handles requests to the administrative
     * user-interface.
     * 
     * @return user-interface controller.
     */
    @Bean
    public UiController uiController() {
        return new UiController();
    }

}
