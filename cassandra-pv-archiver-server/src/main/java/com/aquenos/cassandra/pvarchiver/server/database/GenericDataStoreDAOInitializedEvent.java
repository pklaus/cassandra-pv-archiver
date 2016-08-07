/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.database;

import org.springframework.context.ApplicationEvent;

/**
 * Application event triggered by the {@link GenericDataStoreDAO} when it has
 * been fully initialized. When this event is received, it means that the
 * {@link GenericDataStoreDAO} is now able to serve requests. However,
 * operations might still fail if the connection to the whole cluster is lost in
 * the meantime.
 * 
 * @author Sebastian Marsching
 */
public class GenericDataStoreDAOInitializedEvent extends ApplicationEvent {

    private static final long serialVersionUID = -5712398473371922484L;

    /**
     * Creates an event originating from the specified generic data-store DAO.
     * 
     * @param source
     *            generic data-store DAO that is now ready to be used.
     */
    public GenericDataStoreDAOInitializedEvent(GenericDataStoreDAO source) {
        super(source);
    }

    @Override
    public GenericDataStoreDAO getSource() {
        return (GenericDataStoreDAO) super.getSource();
    }

}
