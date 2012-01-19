/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.reader.cassandra.internal;

/**
 * Provides a source for cancelations. This interface is only intended for
 * internal use by classes in the same bundle.
 * 
 * @author Sebastian Marsching
 * @see Cancelable
 */
public interface CancelationProvider {
	void register(Cancelable cancelable);

	void unregister(Cancelable cancelable);
}
