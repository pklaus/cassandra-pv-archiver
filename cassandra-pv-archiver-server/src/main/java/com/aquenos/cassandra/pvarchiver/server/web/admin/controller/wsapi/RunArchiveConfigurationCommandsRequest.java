/*
 * Copyright 2016-2017 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.web.admin.controller.wsapi;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationCommand;
import com.aquenos.cassandra.pvarchiver.server.archiving.ArchiveConfigurationService;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * <p>
 * Request object for the "run archive configuration commands" function of the
 * web-service API.
 * </p>
 * 
 * <p>
 * This class is primarily intended to facilitate JSON deserialization of the
 * data provided by an API client. The input data is validated to ensure that it
 * conforms to the API specification.
 * </p>
 * 
 * @author Sebastian Marsching
 */
public final class RunArchiveConfigurationCommandsRequest {

    private List<ArchiveConfigurationCommand> commands;

    /**
     * Creates a request.
     * 
     * @param commands
     *            commands to be run by the
     *            {@link ArchiveConfigurationService#runConfigurationCommands(Iterable)}
     *            method.
     * @throws NullPointerException
     *             if <code>commands</code> is <code>null</code> or contains
     *             <code>null</code> elements.
     */
    @JsonCreator
    public RunArchiveConfigurationCommandsRequest(
            @JsonProperty(value = "commands", required = true) List<? extends ArchiveConfigurationCommand> commands) {
        Preconditions.checkNotNull(commands,
                "The commands list must not be null.");
        // We do not have to test the list for null elements, because copyOf(..)
        // will throw a NullPointerException if there are null elements.
        this.commands = ImmutableList.copyOf(commands);
    }

    /**
     * Returns the commands that shall be run by the
     * {@link ArchiveConfigurationService#runConfigurationCommands(Iterable)}
     * method.
     * 
     * @return commands to be run by the archive configuration service.
     */
    public List<ArchiveConfigurationCommand> getCommands() {
        return commands;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(commands).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        RunArchiveConfigurationCommandsRequest other = (RunArchiveConfigurationCommandsRequest) obj;
        return new EqualsBuilder().append(this.commands, other.commands)
                .isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
