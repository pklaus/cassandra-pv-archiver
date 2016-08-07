/*
 * Copyright 2015 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.util;

import java.util.Comparator;
import java.util.UUID;

import com.google.common.primitives.UnsignedLongs;

/**
 * Comparator for comparing {@link UUID}s. Unlike the
 * {@link UUID#compareTo(UUID)} method, this comparator interprets the UUIDs as
 * unsigned integers, thus ordering them in the same order that most non-Java
 * code would use.
 * 
 * @author Sebastian Marsching
 */
public class UUIDComparator implements Comparator<UUID> {

    @Override
    public int compare(UUID uuid1, UUID uuid2) {
        long msb1 = uuid1.getMostSignificantBits();
        long lsb1 = uuid1.getLeastSignificantBits();
        long msb2 = uuid2.getMostSignificantBits();
        long lsb2 = uuid2.getLeastSignificantBits();
        int msbComparison = UnsignedLongs.compare(msb1, msb2);
        return msbComparison != 0 ? msbComparison : UnsignedLongs.compare(lsb1,
                lsb2);
    }

}
