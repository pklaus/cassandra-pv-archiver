/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.writer.cassandra.internal;

import org.csstudio.archive.writer.WriteChannel;

/**
 * Implementation of the {@link WriteChannel} interface for the Cassandra
 * archive support. This class is only intended for use by classes in the same
 * bundle.
 * 
 * @author Sebastian Marsching
 */
public class CassandraWriteChannel implements WriteChannel {

	private String name;

	public CassandraWriteChannel(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

}
