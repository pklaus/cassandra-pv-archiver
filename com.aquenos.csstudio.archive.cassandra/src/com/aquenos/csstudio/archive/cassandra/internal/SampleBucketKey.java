/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import java.math.BigInteger;

import org.csstudio.data.values.ITimestamp;

/**
 * Row key for a bucket of samples. Encapsulates the channel name, the bucket
 * size (in nanoseconds) and the start timestamp of the bucket.
 * 
 * @author Sebastian Marsching
 */
public class SampleBucketKey {

    private String channelName;
    private BigInteger bucketSize;
    private ITimestamp bucketStartTime;

    public SampleBucketKey(String channelName, BigInteger bucketSize,
            ITimestamp bucketStartTime) {
        this.channelName = channelName;
        this.bucketSize = bucketSize;
        this.bucketStartTime = bucketStartTime;
    }

    public String getChannelName() {
        return channelName;
    }

    public BigInteger getBucketSize() {
        return bucketSize;
    }

    public ITimestamp getBucketStartTime() {
        return bucketStartTime;
    }

}
