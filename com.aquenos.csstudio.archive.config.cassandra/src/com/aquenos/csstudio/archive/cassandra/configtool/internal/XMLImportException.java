/*
 * Copyright 2012-2013 aquenos GmbH.
 * Based on the archive config application for RDB archives
 * Copyright (c) 2011 Oak Ridge National Laboratory.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.configtool.internal;

import org.xml.sax.SAXException;

/**
 * Exception used by parser to report an error related to the archive
 * configuration. Other generic parser errors will be reported by the plain
 * SAXException and typically result in a stack dump.
 * 
 * @author Kay Kasemir
 */
public class XMLImportException extends SAXException {
    public XMLImportException(final String message) {
        super(message);
    }

    private static final long serialVersionUID = 1L;
}