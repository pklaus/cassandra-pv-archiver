/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.internal;

import org.csstudio.data.values.ISeverity;

/**
 * Maps a severity to a number and vice-versa. This class is only intended for
 * internal use by other classes in the same bundle.
 * 
 * @author Sebastian Marsching
 */
public abstract class SeverityMapper {

	public static byte severityToNumber(ISeverity severity) {
		byte number;
		if (severity.isOK()) {
			number = 0;
		} else if (severity.isMinor()) {
			number = 1;
		} else if (severity.isMajor()) {
			number = 2;
		} else if (severity.isInvalid()) {
			number = 3;
		} else {
			throw new IllegalArgumentException("Unknown severity: " + severity);
		}
		if (!severity.hasValue()) {
			return (byte) (number - 128);
		} else {
			return number;
		}
	}

	public static ISeverity numberToSeverity(byte number) {

		if (number >= 0 && number <= 3) {
			return new SeverityImpl(number, true);
		} else if (number >= -128 && number <= -125) {
			return new SeverityImpl((byte) (number + 128), false);
		} else {
			throw new IllegalArgumentException(
					"Number representing severity must be between -1 and 3.");
		}
	}

	private static class SeverityImpl implements ISeverity {

		private static final long serialVersionUID = -6232821242029071256L;
		private byte severity;
		private boolean hasValue;

		public SeverityImpl(byte number, boolean hasValue) {
			this.severity = number;
			this.hasValue = hasValue;
		}

		@Override
		public boolean isOK() {
			return severity == 0;
		}

		@Override
		public boolean isMinor() {
			return severity == 1;
		}

		@Override
		public boolean isMajor() {
			return severity == 2;
		}

		@Override
		public boolean isInvalid() {
			return severity == 3;
		}

		@Override
		public boolean hasValue() {
			return hasValue;
		}

		@Override
		public String toString() {
			switch (severity) {
			case 0:
				return "OK";
			case 1:
				return "MINOR";
			case 2:
				return "MAJOR";
			case 3:
				return "INVALID";
			default:
				return "<unknown>";
			}
		}

	}

}
