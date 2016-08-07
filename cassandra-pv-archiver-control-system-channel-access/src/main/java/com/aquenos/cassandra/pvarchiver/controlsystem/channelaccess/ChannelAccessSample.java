/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess;

import com.aquenos.cassandra.pvarchiver.controlsystem.Sample;

/**
 * Sample for the {@link ChannelAccessControlSystemSupport}. This interface only
 * acts as a marker because there are different implementations for raw,
 * aggregated, and disconnected samples.
 * 
 * @author Sebastian Marsching
 *
 */
public interface ChannelAccessSample extends Sample {

    /**
     * Tells whether this sample is an original sample that has been received
     * over the network. This information is used when serializing samples to
     * JSON in order to correctly mark whether samples are <code>ORIGINAL</code>
     * or <code>INTERPOLATED</code>.
     * 
     * @return <code>true</code> if this sample is an original sample that has
     *         been received over the network, <code>false</code> if it a sample
     *         that has been generated as the result of a decimation process.
     */
    boolean isOriginalSample();

    /**
     * Returns the sample's type. The type indicates the kind of value that is
     * stored inside this sample.
     * 
     * @return this sample's type.
     */
    ChannelAccessSampleType getType();

}
