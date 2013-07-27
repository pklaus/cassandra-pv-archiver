/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import java.util.concurrent.atomic.AtomicReference;

import com.aquenos.csstudio.archive.config.cassandra.CassandraArchiveConfig;

/**
 * Holds the name of the archive engine. This name is set when an engine
 * configuration is retrieved for the first time. In an archive engine / writer
 * environment this typically is the name of the archive engine this VM is
 * responsible for. In other environments the stored value might not have a
 * special meaning or might always be <code>null</code>.
 * 
 * This class is only a work-around for the fact that the archive-engine
 * framework does not tell the archive writer its engine name. It should not be
 * used by third-party code and might be removed in the future without further
 * notice.
 * 
 * @author Sebastian Marsching
 * 
 */
public final class EngineNameHolder {

    private final static AtomicReference<String> ENGINE_NAME = new AtomicReference<String>();

    private EngineNameHolder() {
    }

    /**
     * Returns the name of the engine that has been supplied on the first call
     * to {@link CassandraArchiveConfig#findEngine(String)}. If this method has
     * not been called yet, <code>null</code> is returned.
     * 
     * @return name of the archive engine for the current environment. Only
     *         valid under certain assumptions (see comments for this class).
     */
    public static String getEngineName() {
        return ENGINE_NAME.get();
    }

    /**
     * Sets the engine name if it has not been set yet. This method should only
     * be called by {@link CassandraArchiveConfig#findEngine(String)}.
     * 
     * @param engineName
     *            name of the engine the configuration has been requested for.
     */
    public static void setEngineName(String engineName) {
        ENGINE_NAME.compareAndSet(null, engineName);
    }

}
