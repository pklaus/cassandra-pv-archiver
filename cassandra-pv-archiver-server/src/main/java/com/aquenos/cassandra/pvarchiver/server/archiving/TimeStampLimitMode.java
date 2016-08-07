/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.archiving;

/**
 * Mode specifying how a time-stamp limit shall be interpreted.
 * 
 * @author Sebastian Marsching
 */
public enum TimeStampLimitMode {

    /**
     * The specified limit shall be interpreted so that a sample at or - if
     * there is no sample at the specified limit - a sample after the specified
     * limit is included.
     */
    AT_OR_AFTER,

    /**
     * The specified limit shall be interpreted so that a sample at or - if
     * there is no sample at the specified limit - a sample before the specified
     * limit is included.
     */
    AT_OR_BEFORE;

}
