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
 * Very simple implementation of a sample writer delegate that directly
 * delegates to a {@link SampleListener}. As this implementation is so simply,
 * its {@link #destroy()} method is a no-op. This means that samples might be
 * written after the <code>destroy()</code> method has been called. However,
 * this will only happen when the {@link #writeSample(ChannelAccessSample, int)}
 * method is called after calling <code>destroy()</code>. This implementation
 * does not schedule any tasks to be run in the future but only writes samples
 * directly from the <code>writeSample(...)</code> method.
 * </p>
 * 
 * <p>
 * This class is intended for internal use by
 * {@link ChannelAccessArchivingChannel} and its associated classes only.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class SimpleSampleWriterDelegate implements SampleWriterDelegate {

    private final ChannelAccessArchivingChannel channel;
    private final SampleListener<ChannelAccessSample> sampleListener;

    /**
     * Creates a sample write delegate that directly delegates to the specified
     * <code>sampleListener</code>. The specified <code>channel</code> is passed
     * to the <code>sampleListener</code>.
     * 
     * @param channel
     *            channel for which this sample writer writes samples. This is
     *            the channel that is passed to the <code>sampleListener</code>.
     * @param sampleListener
     *            sample listener that is used for actually writing samples.
     */
    public SimpleSampleWriterDelegate(ChannelAccessArchivingChannel channel,
            SampleListener<ChannelAccessSample> sampleListener) {
        this.channel = channel;
        this.sampleListener = sampleListener;
    }

    @Override
    public void destroy() {
        // For this simple implementation, we do not have to destroy anything.
    }

    @Override
    public void writeSample(ChannelAccessSample sample, int estimatedSampleSize) {
        sampleListener.onSampleReceived(channel, sample, estimatedSampleSize);
    }

}
