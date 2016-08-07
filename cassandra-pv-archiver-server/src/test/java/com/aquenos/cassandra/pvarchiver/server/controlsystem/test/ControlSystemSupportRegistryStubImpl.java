/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.controlsystem.test;

import java.util.Collection;
import java.util.Collections;

import com.aquenos.cassandra.pvarchiver.controlsystem.ControlSystemSupport;
import com.aquenos.cassandra.pvarchiver.server.controlsystem.ControlSystemSupportRegistry;

/**
 * Stub implementation of the {@link ControlSystemSupportRegistry}. This
 * implementation simply returns a single control-system support, passed on
 * construction.
 * 
 * @author Sebastian Marsching
 */
public class ControlSystemSupportRegistryStubImpl implements
        ControlSystemSupportRegistry {

    private ControlSystemSupport<?> controlSystemSupport;

    /**
     * Creates a control-system support registry, that provides exactly one
     * control-system support.
     * 
     * @param controlSystemSupport
     *            control-system support provided by this registry.
     */
    public ControlSystemSupportRegistryStubImpl(
            ControlSystemSupport<?> controlSystemSupport) {
        this.controlSystemSupport = controlSystemSupport;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public ControlSystemSupport<?> getControlSystemSupport(String id) {
        if (controlSystemSupport.getId().equals(id)) {
            return controlSystemSupport;
        } else {
            return null;
        }
    }

    @Override
    public Collection<? extends ControlSystemSupport<?>> getControlSystemSupports() {
        return Collections.singleton(controlSystemSupport);
    }

}
