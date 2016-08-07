/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem;

/**
 * Control-system side of an archived channel. This interface provides the
 * methods through which the archiver interacts with the control-system-specific
 * side of each archived channel. Methods can be created through the
 * {@link ControlSystemSupport#createChannel(String, java.util.Map, SampleBucketId, SampleListener)}
 * method.
 * 
 * @author Sebastian Marsching
 *
 */
public interface ControlSystemChannel {

    /**
     * Destroys the control-system channel. This means that all resources
     * associated with the channel in the control-system library should be freed
     * and no further samples should be sent to the {@link SampleListener}
     * associated with this channel. This method should not block, so the
     * destruction should be implemented in an asynchronous way, if blocking
     * operations are required. It is okay if a channel keeps sending samples,
     * even after returning from this method.
     */
    void destroy();

    /**
     * Returns the channel name identifying this channel. This must be the name
     * that was specified when creating this channel through the
     * {@link ControlSystemSupport#createChannel(String, java.util.Map, SampleBucketId, SampleListener)}
     * method.
     * 
     * @return name identifying this channel.
     */
    String getChannelName();

    /**
     * <p>
     * Returns the channel status. The state can be connected, disconnected, or
     * error. In the error state, the status can also contain an optional error
     * message.
     * </p>
     * 
     * <p>
     * The error state should be reserved for problems that are non-recoverable
     * (e.g. configuration problems). If the channel is simply temporarily
     * unavailable (e.g. because the corresponding server is not started), the
     * disconnected state should be preferred.
     * </p>
     * 
     * <p>
     * This method never returns <code>null</code>.
     * </p>
     * 
     * @return status of this channel.
     */
    ControlSystemChannelStatus getStatus();

}
