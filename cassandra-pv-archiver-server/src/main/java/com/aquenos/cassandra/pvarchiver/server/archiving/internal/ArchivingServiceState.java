/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving.internal;

import java.util.SortedMap;

/**
 * <p>
 * Encapsulates the state of the {@link ArchivingServiceInternalImpl}. The whole
 * state is stored in a single object so that it can be updated atomically
 * without needing a mutex. Instances of this class are immutable so that there
 * is no risk of concurrent modifications.
 * </p>
 * 
 * <p>
 * This class is only intended to be used by the
 * {@link ArchivingServiceInternalImpl}, the {@link ArchivedChannel}, and the
 * {@link ArchivedChannelDecimationLevel} classes. For this reason, it is
 * package private.
 * </p>
 * 
 * @author Sebastian Marsching
 */
class ArchivingServiceState {

    // Implementation note: An ArchivingManagerState object is never
    // modified. This allows us to access such an object without needing a
    // mutex. Instead of updating such an object, a new object is created
    // and replaces the old one in a CAS operation.

    private final boolean initialized;
    private final boolean online;
    // We use a tree map so that the channels are sorted by their names.
    // This makes the get and put operations slightly more expensive than for a
    // HashMap (O(log n) instead of O(1)), but for the typical number of
    // channels, we should be fine and it makes use in the UI much simpler
    // and faster.
    private final SortedMap<String, ArchivedChannel<?>> channels;

    /**
     * Creates a new state instance that is initialized with the specified state
     * information. Instances of this class should only be created by the
     * {@link ArchivingServiceInternalImpl}.
     * 
     * @param initialized
     *            <code>true</code> if the initialization of the
     *            {@link ArchivingServiceInternalImpl} has been finished, and
     *            the service has not switched offline. This implies that the
     *            server is also <code>online</code>. Please note that the
     *            service having been initialized does not necessarily mean that
     *            all its channels have been initialized, too. It just means
     *            that they have been created and initialization has started.
     * @param online
     *            <code>true</code> if the {@link ArchivingServiceInternalImpl}
     *            is online, <code>false</code> if it is offline. This flag must
     *            be <code>true</code> if <code>initialized</code> is
     *            <code>true</code>.
     * @param channels
     *            map containing all the {@link ArchivedChannel} instances
     *            managed by the {@link ArchivingServiceInternalImpl}, each one
     *            identified by its channel name. This map must be immutable.
     *            The calling code has to ensure this by either creating a
     *            immutable map or by wrapping a mutable map in an unmodifiable
     *            one and ensuring that no further modifications are made to the
     *            original map after passing it to this constructor.
     */
    ArchivingServiceState(boolean initialized, boolean online,
            SortedMap<String, ArchivedChannel<?>> channels) {
        // When the service is initialized, it is also online. We only use an
        // assertion here because the calling code should ensure that this
        // condition holds.
        assert (!initialized || online);
        this.initialized = initialized;
        this.online = online;
        // We assume that the caller ensures that the channels map is immutable.
        // We do not copy it for performance reasons.
        this.channels = channels;
    }

    /**
     * Tells whether the {@link ArchivingServiceInternalImpl} has been
     * initialized and has not been switched offline yet. If this method returns
     * <code>true</code>, this implies that {@link #isOnline()} also returns
     * <code>true</code>. Please note that the service having been initialized
     * does not necessarily mean that all its channels have been initialized,
     * too. It just means that they have been created and initialization has
     * started.
     * 
     * @return <code>true</code> if the archiving service has finished
     *         initialization and has not been switched offline since, false if
     *         it is offline or it has just been switched online and has not
     *         finished initialization yet.
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Tells whether the {@link ArchivingServiceInternalImpl} has been switched
     * online and has not been switched offline yet. Even if the archiving
     * service is online, it might not have been fully initialized yet. Use
     * {@link #isInitialized()} to check whether the service has also finished
     * initialization.
     * 
     * @return <code>true</code> if the archiving service is online,
     *         <code>false</code> if it is offline.
     */
    public boolean isOnline() {
        return online;
    }

    /**
     * Returns the map containing all the channels managed by the
     * {@link ArchivingServiceInternalImpl}. Each entry in this map uses the
     * channel's name as the key and the corresponding {@link ArchivedChannel}
     * object as its value. This map is immutable and represents the channels
     * managed by the archiving service at the point in time when this state
     * object was created. However, the channel instances contained in this map
     * are <em>not</em> immutable and care must be taken to synchronize on them
     * when calling methods that depend on their mutable state. See the
     * documentation of {@link ArchivedChannel} for details.
     * 
     * @return map containing the channels managed by the archiving service,
     *         each one being identified by its channel name.
     */
    public SortedMap<String, ArchivedChannel<?>> getChannels() {
        return channels;
    }

}
