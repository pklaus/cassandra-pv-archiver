/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.common.spring;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.LocaleResolver;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Locale resolver that selects the locale based on the
 * <code>Accept-Language</code> header of the request. Unlike Spring's
 * <code>AcceptHeaderLocaleResolver</code>, this implementation does not simply
 * use the preferred locale from the request. Instead, it compares the list of
 * locales requested by the client with a set of supported locales, choosing the
 * most preferred locale that is also supported. If none of the locales
 * requested by the client are supported, this implementation falls back to
 * using a default locale.
 * 
 * @author Sebastian Marsching
 */
public class AcceptHeaderLocaleResolver implements LocaleResolver {

    private Locale defaultLocale = Locale.getDefault();
    private Set<Locale> supportedLocales = Collections.emptySet();

    @Override
    public Locale resolveLocale(HttpServletRequest request) {
        Enumeration<Locale> localeEnumeration = request.getLocales();
        while (localeEnumeration.hasMoreElements()) {
            Locale locale = localeEnumeration.nextElement();
            if (supportedLocales.contains(locale)) {
                return locale;
            }
        }
        return defaultLocale;
    }

    @Override
    public void setLocale(HttpServletRequest request,
            HttpServletResponse response, Locale locale) {
        throw new UnsupportedOperationException(
                "Cannot change HTTP accept header - use a different locale resolution strategy");
    }

    /**
     * Returns the default locale. The default locale is used when none of the
     * locales requested by the clients are supported. If not configured
     * explicitly, the default locale is the platform's default locale as
     * returned by {@link Locale#getDefault()}. The default locale is never
     * null.
     * 
     * @return default locale used when none of the requested locales match any
     *         of the supported locales.
     * @see #getSupportedLocales()
     */
    public Locale getDefaultLocale() {
        return defaultLocale;
    }

    /**
     * Sets the default locale. The default locale is used when none of the
     * locales requested by the clients are supported. If not configured
     * explicitly, the default locale is the platform's default locale as
     * returned by {@link Locale#getDefault()}.
     * 
     * @param defaultLocale
     *            default locale to be used when none of the requested locales
     *            match any of the supported locales.
     * @see #setSupportedLocales(Set)
     * @throws NullPointerException
     *             if <code>defaultLocale</code> is <code>null</code>.
     */
    public void setDefaultLocale(Locale defaultLocale) {
        Preconditions.checkNotNull(defaultLocale);
        this.defaultLocale = defaultLocale;
    }

    /**
     * Returns the set of supported locales. This locale resolver compares the
     * list of locales requested by the client against this set and uses the
     * first requested locale that is supported. If none of the locales is
     * supported, it falls back to the default locale. If not configured
     * explicitly, the set of supported locales is empty, resulting in the
     * default locale to be always selected. The set of supported locales is
     * never <code>null</code>.
     * 
     * @return set of supported locales.
     * @see #getDefaultLocale()
     */
    public Set<Locale> getSupportedLocales() {
        return supportedLocales;
    }

    /**
     * Replaces the set of supported locales. This locale resolver compares the
     * list of locales requested by the client against this set and uses the
     * first requested locale that is supported. If none of the locales is
     * supported, it falls back to the default locale. If not configured
     * explicitly, the set of supported locales is empty, resulting in the
     * default locale to always be selected.
     * 
     * @param supportedLocales
     *            set of supported locales.
     * @throws NullPointerException
     *             if <code>supportedLocales</code> is <code>null</code> or
     *             contains <code>null</code> elements.
     */
    public void setSupportedLocales(Set<Locale> supportedLocales) {
        this.supportedLocales = ImmutableSet.copyOf(supportedLocales);
    }

}
