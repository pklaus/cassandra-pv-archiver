/*
 * Copyright 2013 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

/**
 * Response to a {@link CompressionRequest}.
 * 
 * @author Sebastian Marsching
 * 
 */
public class CompressionResponse {

    /**
     * Request this response corresponds to.
     */
    public CompressionRequest request;

    /**
     * Request was processed successfully (no error occurred).
     */
    public boolean success;

}
