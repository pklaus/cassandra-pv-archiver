/*
 * Copyright 2016 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.cassandra.pvarchiver.server.internode;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * <p>
 * Response sent in reply to a request to get the current system time.
 * </p>
 * 
 * <p>
 * This response object is a simple wrapper around a <code>long</code>. However,
 * it is needed because want to enforce JSON serialization as a string
 * (serializing 64-bit integers as JSON integers can be dangerous) and we want
 * the response to be kind of self describing.
 * </p>
 * 
 * @author Sebastian Marsching
 *
 */
public final class CurrentSystemTimeResponse {

    private long currentSystemTimeMilliseconds;

    /**
     * Creates a current-system-time response.
     * 
     * @param currentSystemTimeMilliseconds
     *            the current system time in milliseconds. When creating the
     *            response (not deserializing it), this should be set to the
     *            return value of {@link System#currentTimeMillis()}.
     */
    @JsonCreator
    public CurrentSystemTimeResponse(
            @JsonProperty(value = "currentSystemTimeMilliseconds", required = true) long currentSystemTimeMilliseconds) {
        this.currentSystemTimeMilliseconds = currentSystemTimeMilliseconds;
    }

    /**
     * Returns the current system time on the server at the time when the
     * response was created. The current system time is determined by calling
     * {@link System#currentTimeMillis()}.
     * 
     * @return return value of {@link System#currentTimeMillis()} at the time
     *         when the response was created on the server that was queried.
     */
    @JsonSerialize(using = ToStringSerializer.class)
    public long getCurrentSystemTimeMilliseconds() {
        return currentSystemTimeMilliseconds;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(currentSystemTimeMilliseconds)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }
        CurrentSystemTimeResponse other = (CurrentSystemTimeResponse) obj;
        return new EqualsBuilder().append(this.currentSystemTimeMilliseconds,
                other.currentSystemTimeMilliseconds).isEquals();
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
