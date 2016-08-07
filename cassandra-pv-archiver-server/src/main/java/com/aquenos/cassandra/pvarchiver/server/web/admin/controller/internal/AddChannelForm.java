/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.internal;

import java.util.UUID;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.aquenos.cassandra.pvarchiver.server.web.admin.controller.UiController;

/**
 * <p>
 * Form-backing bean for adding channels.
 * </p>
 * 
 * <p>
 * This class is only intended for use by the {@link UiController} and its
 * associated views.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public class AddChannelForm extends AbstractAddEditChannelForm {

    @NotNull
    @Size(min = 1)
    private String channelName;
    @NotNull
    private String controlSystemType;
    @NotNull
    private UUID serverId;

    /**
     * Returns the channel name.
     * 
     * @return channel name or <code>null</code> if this bean has not been
     *         properly initialized yet.
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Sets the channel name. A <code>null</code> or empty value is acceptable,
     * however this will later result in a validation error.
     * 
     * @param channelName
     *            channel name (may be <code>null</code>).
     */
    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    /**
     * Returns the control-system type.
     * 
     * @return control-system type or <code>null</code> if this bean has not
     *         been properly initialized yet.
     */
    public String getControlSystemType() {
        return controlSystemType;
    }

    /**
     * Sets the control-system type. A <code>null</code> value is acceptable,
     * however this will later result in a validation error.
     * 
     * @param controlSystemType
     *            control-system type (may be <code>null</code>).
     */
    public void setControlSystemType(String controlSystemType) {
        this.controlSystemType = controlSystemType;
    }

    /**
     * Returns the ID of the server to which the channel shall be added.
     * 
     * @return server ID or <code>null</code> if this bean has not been properly
     *         initialized yet.
     */
    public UUID getServerId() {
        return serverId;
    }

    /**
     * Sets the ID of the server to which the channel shall be added. A
     * <code>null</code> value is acceptable, however this will later result in
     * a validation error.
     * 
     * @param serverId
     *            server ID (may be <code>null</code>).
     */
    public void setServerId(UUID serverId) {
        this.serverId = serverId;
    }

}
