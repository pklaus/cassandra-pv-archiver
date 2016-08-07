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
 * Base interface for control-system samples. This interface must be implemented
 * by all control-systems and only provides a time-stamp. All actual sample data
 * depends on the control-system and is thus left to the implementation.
 * 
 * @author Sebastian Marsching
 */
public interface Sample {

    /**
     * Returns the time stamp associated with the sample. The time stamp is
     * returned as the number of nanoseconds since epoch (January 1st, 1970,
     * 00:00:00 UTC).
     * 
     * @return time stamp associated with this sample.
     */
    long getTimeStamp();

}
