/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * <p>
 * Configuration properties that are only used in development mode. This object
 * is injected with properties having the <code>developmentMode.*</code> prefix.
 * </p>
 * 
 * <p>
 * The <code>developmentMode.enabled</code> property allows setting various
 * options that are only desirable during development (e.g. turning off caches)
 * in a single place.
 * </p>
 * 
 * <p>
 * Instances of this class are safe for concurrent read access but are not safe
 * for concurrent write access. Typically, this should not be a problem because
 * an instance of this class is initialized once at application startup and then
 * only used for read access.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 */
@ConfigurationProperties(prefix = "developmentMode", ignoreUnknownFields = false)
public class DevelopmentModeProperties {

    private boolean enabled = false;

    /**
     * Tells whether the development mode is enabled.
     * 
     * @return <code>true</code> if the development mode is enabled,
     *         <code>false</code> if running in production mode.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the development mode. Typically, this method is called by the Spring
     * container when populating this object from the configuration properties
     * in its environment. The default value is <code>false</code>.
     * 
     * @param enabled
     *            <code>true</code> if the development mode should be enabled,
     *            <code>false</code> if it should be disabled.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

}
