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
 * Interface for an object that can be canceled. This interface is only intended
 * for internal use by classes in the same bundle.
 * 
 * @author Sebastian Marsching
 * @see CancelationProvider
 */
public interface Cancelable {
	void cancel();
}
