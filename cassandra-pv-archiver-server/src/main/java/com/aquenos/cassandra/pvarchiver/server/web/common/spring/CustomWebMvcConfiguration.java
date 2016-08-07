/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.config.annotation.DelegatingWebMvcConfiguration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * <p>
 * Configuration that is roughly equivalent to the {@link EnableWebMvc}
 * annotation, but uses slightly different default settings.
 * </p>
 * 
 * <p>
 * This configuration makes a few modifications to the configuration used by
 * <code>@EnableWebMvc</code>.
 * </p>
 * <ul>
 * <li>It sets <code>true</code> for
 * {@link RequestMappingHandlerAdapter#setIgnoreDefaultModelOnRedirect(boolean)}
 * , thus ensuring that model parameters are not appended to a redirect URL.
 * This is actually the setting recommended by the Spring documentation, but it
 * is not the default used by Spring due to backward compatibility issues.
 * <li>It replaces the {@link RequestMappingHandlerMapping} with
 * {@link PublicOnlyRequestMappingHandlerMapping}. Using this modified mapping
 * ensures that only public methods are used to handle requests.
 * {@link RequestMapping} annotations on other methods are ignored and a warning
 * message is logged. Using <code>@RequestMapping</code> with non-public methods
 * is dangerous, because other annotations that rely on AOP proxying might get
 * ignored on such methods.</li>
 * <li>It sets the <code>removeSemicolonContent</code> property of the
 * {@link RequestMappingHandlerMapping} to <code>false</code>, so that path
 * variables that contain a semicolon are handled correctly.</li>
 * </ul>
 * 
 * @author Sebastian Marsching
 */
@Configuration
public class CustomWebMvcConfiguration extends DelegatingWebMvcConfiguration {

    @Bean
    @Override
    public RequestMappingHandlerAdapter requestMappingHandlerAdapter() {
        RequestMappingHandlerAdapter adapter = super
                .requestMappingHandlerAdapter();
        adapter.setIgnoreDefaultModelOnRedirect(true);
        return adapter;
    }

    @Bean
    @Override
    public RequestMappingHandlerMapping requestMappingHandlerMapping() {
        RequestMappingHandlerMapping handlerMapping = new PublicOnlyRequestMappingHandlerMapping();
        handlerMapping.setOrder(0);
        handlerMapping.setInterceptors(getInterceptors());
        handlerMapping
                .setContentNegotiationManager(mvcContentNegotiationManager());
        handlerMapping.setRemoveSemicolonContent(false);

        PathMatchConfigurer configurer = getPathMatchConfigurer();
        if (configurer.isUseSuffixPatternMatch() != null) {
            handlerMapping.setUseSuffixPatternMatch(configurer
                    .isUseSuffixPatternMatch());
        }
        if (configurer.isUseRegisteredSuffixPatternMatch() != null) {
            handlerMapping.setUseRegisteredSuffixPatternMatch(configurer
                    .isUseRegisteredSuffixPatternMatch());
        }
        if (configurer.isUseTrailingSlashMatch() != null) {
            handlerMapping.setUseTrailingSlashMatch(configurer
                    .isUseTrailingSlashMatch());
        }
        if (configurer.getPathMatcher() != null) {
            handlerMapping.setPathMatcher(configurer.getPathMatcher());
        }
        if (configurer.getUrlPathHelper() != null) {
            handlerMapping.setUrlPathHelper(configurer.getUrlPathHelper());
        }

        return handlerMapping;
    }

}
