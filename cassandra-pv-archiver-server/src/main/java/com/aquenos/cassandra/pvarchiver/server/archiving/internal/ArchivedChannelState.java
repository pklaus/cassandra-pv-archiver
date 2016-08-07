/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

/**
 * Enum representing the states an channel can take internally. Used by
 * {@link ArchivedChannel}, {@link ArchivedChannelDecimationLevel}, and
 * {@link ArchivingServiceInternalImpl}. This class is only intended to be used
 * by these classes and has thus been made package-private.
 * 
 * @author Sebastian Marsching
 */
enum ArchivedChannelState {

    /**
     * The channel has been created, but has not been initialized yet.
     */
    INITIALIZING,

    /**
     * The channel has been initialized and not destroyed yet. However,
     * destruction may already have started.
     */
    INITIALIZED,

    /**
     * The channel has been destroyed. If there was an error during
     * initialization, a channel might change directly from the
     * {@link #INITIALIZING} to this state. Otherwise, the channel has been in
     * the {@link #INITIALIZED} state before changing to this state.
     */
    DESTROYED

}
