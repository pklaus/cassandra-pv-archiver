/*
 * Copyright 2015-2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.controlsystem.channelaccess.internal;

import org.apache.commons.logging.LogFactory;

import com.aquenos.epics.jackie.common.exception.ErrorHandler;

/**
 * Error handler implementation that delegates the handling of errors that occur
 * in the Channel Access client to the Apache Commons Logging framework. This is
 * the logging framework used by Spring, so it is available in the archive
 * server application.
 * 
 * @author Sebastian Marsching
 */
public class CommonsLoggingErrorHandler implements ErrorHandler {

    @Override
    public void handleError(Class<?> context, Throwable e, String description) {
        if (description == null) {
            description = "An error occurred in the Channel Access client library.";
        }
        if (e == null) {
            LogFactory.getLog(context).error(description);
        } else {
            LogFactory.getLog(context).error(description, e);
        }
    }

}
