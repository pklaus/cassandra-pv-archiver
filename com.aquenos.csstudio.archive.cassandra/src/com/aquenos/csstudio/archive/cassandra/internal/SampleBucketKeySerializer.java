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
import java.nio.ByteBuffer;

import org.csstudio.data.values.ITimestamp;

import com.aquenos.csstudio.archive.cassandra.util.TimestampSerializer;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.ComparatorType;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Serializer and deserializer for {@link SampleBucketKey} instances.
 * 
 * @author Sebastian Marsching
 */
public class SampleBucketKeySerializer extends
        AbstractSerializer<SampleBucketKey> {

    private final static SampleBucketKeySerializer INSTANCE = new SampleBucketKeySerializer();

    public static SampleBucketKeySerializer get() {
        return INSTANCE;
    }

    /**
     * Constructor. This is protected, because the {@link #get()} method should
     * be used to get an instance (singleton pattern).
     */
    protected SampleBucketKeySerializer() {
    }

    @Override
    public ByteBuffer toByteBuffer(SampleBucketKey sampleBucketKey) {
        Composite composite = new Composite();
        composite.addComponent(sampleBucketKey.getChannelName(),
                StringSerializer.get());
        composite.addComponent(sampleBucketKey.getBucketSize(),
                BigIntegerSerializer.get());
        composite.addComponent(sampleBucketKey.getBucketStartTime(),
                TimestampSerializer.get());
        return composite.serialize();
    }

    @Override
    public SampleBucketKey fromByteBuffer(ByteBuffer byteBuffer) {
        Composite composite = new Composite();
        composite.setComparatorsByPosition(
                ComparatorType.UTF8TYPE.getTypeName(),
                ComparatorType.INTEGERTYPE.getTypeName(), TimestampSerializer
                        .get().getComparatorType().getTypeName());
        composite.deserialize(byteBuffer);
        String channelName = composite.get(0, StringSerializer.get());
        BigInteger bucketSize = composite.get(1, BigIntegerSerializer.get());
        ITimestamp bucketStartTime = composite
                .get(2, TimestampSerializer.get());
        return new SampleBucketKey(channelName, bucketSize, bucketStartTime);
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "Composite columns can't be paginated this way.  Use SpecificCompositeSerializer instead.");
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.COMPOSITETYPE;
    }

}
