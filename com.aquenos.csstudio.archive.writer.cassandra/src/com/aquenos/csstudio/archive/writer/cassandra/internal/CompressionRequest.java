/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import java.util.Deque;
import java.util.Map;

import com.aquenos.csstudio.archive.cassandra.Sample;

/**
 * Request to compress samples (and delete old samples) for a channel. This
 * object stores the channel name as well as state information that needs to be
 * retained between consecutive runs.
 * 
 * @author Sebastian Marsching
 * 
 */
public class CompressionRequest {

    /**
     * Name of the channel that shall be processed.
     */
    public String channelName;

    /**
     * State retained from the last compression of the channel.
     */
    public CompressorPerChannelState compressorState;

    /**
     * Queue samples for the various compression levels.
     */
    public Map<Long, ? extends Deque<Sample>> sourceSamples;

    /**
     * Information about whether source samples have been lost.
     */
    public Map<Long, Boolean> sourceSamplesLost;

    /**
     * Delete old samples. If not set, compressed samples are created only.
     */
    public boolean deleteOldSamples;

}
