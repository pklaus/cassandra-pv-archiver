/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.csstudio.data.values.ITimestamp;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

/**
 * Serializer and deserializer for {@link ITimestamp} objects. This serializer
 * can be used with Astyanax for storing an {@link ITimestamp} where an
 * IntegerType {@link BigInteger} is exepected.
 * 
 * @author Sebastian Marsching
 */
public class TimestampSerializer extends AbstractSerializer<ITimestamp> {

    private final static TimestampSerializer INSTANCE = new TimestampSerializer();

    private TimestampSerializer() {
    }

    /**
     * Returns the only instance of this serializer (singleton pattern).
     * 
     * @return timestamp serializer.
     */
    public static TimestampSerializer get() {
        return INSTANCE;
    }

    @Override
    public ComparatorType getComparatorType() {
        return ComparatorType.INTEGERTYPE;
    }

    @Override
    public ByteBuffer fromString(String str) {
        return BigIntegerSerializer.get().fromString(str);
    }

    @Override
    public String getString(ByteBuffer byteBuffer) {
        return BigIntegerSerializer.get().toString();
    }

    @Override
    public ByteBuffer getNext(ByteBuffer byteBuffer) {
        return BigIntegerSerializer.get().getNext(byteBuffer);
    }

    @Override
    public ByteBuffer toByteBuffer(ITimestamp timestamp) {
        return BigIntegerSerializer.get().toByteBuffer(
                TimestampArithmetics.timestampToBigInteger(timestamp));
    }

    @Override
    public ITimestamp fromByteBuffer(ByteBuffer byteBuffer) {
        return TimestampArithmetics.bigIntegerToTimestamp(BigIntegerSerializer
                .get().fromByteBuffer(byteBuffer));
    }

}
