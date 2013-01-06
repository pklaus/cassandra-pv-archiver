/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.config.cassandra.internal;

import java.nio.ByteBuffer;

import com.aquenos.csstudio.archive.cassandra.util.Pair;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ComparatorType;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * Serializer and deserializer for string pairs. This used for some column
 * families that use a string pair as their row key. This class is only intended
 * for internal use by other classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public class StringPairSerializer extends
        AbstractSerializer<Pair<String, String>> {

    private final static StringPairSerializer INSTANCE = new StringPairSerializer();

    public static StringPairSerializer get() {
        return INSTANCE;
    }

    /**
     * Constructor. This is protected, because the {@link #get()} method should
     * be used to get an instance (singleton pattern).
     */
    protected StringPairSerializer() {
    }

    @Override
    public ByteBuffer toByteBuffer(Pair<String, String> stringPair) {
        Composite composite = new Composite();
        composite.addComponent(stringPair.getFirst(), StringSerializer.get());
        composite.addComponent(stringPair.getSecond(), StringSerializer.get());
        return composite.serialize();
    }

    @Override
    public Pair<String, String> fromByteBuffer(ByteBuffer byteBuffer) {
        Composite composite = new Composite();
        composite.setComparatorsByPosition(
                ComparatorType.UTF8TYPE.getTypeName(),
                ComparatorType.UTF8TYPE.getTypeName());
        composite.deserialize(byteBuffer);
        String firstString = composite.get(0, StringSerializer.get());
        String secondString = composite.get(1, StringSerializer.get());
        return new Pair<String, String>(firstString, secondString);
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
