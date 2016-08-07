/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import com.aquenos.cassandra.pvarchiver.controlsystem.SampleListener;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessArchivingChannel;
import com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.ChannelAccessSample;

/**
 * <p>
 * Common interface for sample writer delegates. A sample writer delegate is
 * responsible for writing the samples that have been received for a channel,
 * delegating to a {@link SampleListener}. There are two implementations of this
 * interface: The {@link SimpleSampleWriterDelegate} directly delegates to the
 * <code>SampleListener</code> each time the <code>writeSample(...)</code>
 * method is called. The {@link LimitingSampleWriterDelegate} on the other hand
 * enforces (upper and lower) limits on the update rate.
 * </p>
 * 
 * <p>
 * The two separate implementations exist so that in the common case, when there
 * is neither an upper nor a lower limit on the update rate, samples can be
 * archived without the additional overhead incurred by the checks that are
 * needed to enforce those limits.
 * </p>
 * 
 * <p>
 * This interface is intended for internal use by
 * {@link ChannelAccessArchivingChannel} and its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public interface SampleWriterDelegate {

    /**
     * Destroys the sample writer delegate. In particular, this means that the
     * sample write shall not write samples any longer and shall not schedule
     * tasks to be executed in the future.
     */
    void destroy();

    /**
     * Writes the specified sample with the specified estimated size. When the
     * sample writer enforces limits on the update rate, the sample might not
     * actually be written but only stored temporarily. When another sample
     * arrives quickly, the temporarily stored sample might be discarded without
     * being written. In the same way, a sample passed to this method might
     * actually be written more than once, using an updated time-stamp when this
     * method is not called again for a sufficiently long period of time.
     * 
     * @param sample
     *            sample to be written.
     * @param estimatedSampleSize
     *            estimated serialized size of the sample to be written (in
     *            bytes).
     */
    void writeSample(ChannelAccessSample sample, int estimatedSampleSize);

}
