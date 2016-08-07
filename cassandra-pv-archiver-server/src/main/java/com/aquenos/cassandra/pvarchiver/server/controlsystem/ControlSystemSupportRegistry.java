/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.controlsystem;

import java.util.Collection;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;

/**
 * Registry providing access to {@link ControlSystemSupport}s. The registry can
 * be used by components that need to access a control system in order to get
 * the support module for a specific control system (using
 * {@link #getControlSystemSupport(String)}) or to get a list of all supported
 * control-systems (using {@link #getControlSystemSupports()}).
 * 
 * @author Sebastian Marsching
 */
public interface ControlSystemSupportRegistry {

    /**
     * Tells whether the registry is available (has been initialized and not
     * destroyed yet). Even if the registry is not available, its method can be
     * called safely. However, it typically will not be able to provide any
     * control-system supports unless it is available.
     * 
     * @return <code>true</code> if the registry is ready to provide
     *         control-system supports, <code>false</code> otherwise.
     */
    boolean isAvailable();

    /**
     * Returns the control-system support with the specified identifier. The
     * identifier is the one returned by the
     * {@link ControlSystemSupport#getId()} method.
     * 
     * @param id
     *            identifier of the control-system support module.
     * @return the requested control-system support module or <code>null</code>
     *         if the requested module has not been registered with this
     *         registry.
     */
    ControlSystemSupport<?> getControlSystemSupport(String id);

    /**
     * Returns all control-system support modules registered with this registry.
     * For example, this can be used to present a list of supported control
     * systems to the user.
     * 
     * @return list of control-system support modules that have been registered
     *         with this registry. Might be empty, but is never
     *         <code>null</code>.
     */
    Collection<? extends ControlSystemSupport<?>> getControlSystemSupports();

}
