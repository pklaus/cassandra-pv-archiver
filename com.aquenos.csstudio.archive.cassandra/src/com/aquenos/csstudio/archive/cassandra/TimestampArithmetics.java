/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra;

import org.csstudio.data.values.ITimestamp;
import org.csstudio.data.values.TimestampFactory;

/**
 * Provides arithmetic operations for timestamps.
 * 
 * @author Sebastian Marsching
 * @see TimestampFactory
 */
public abstract class TimestampArithmetics {

	/**
	 * The earliest timestamp supported. This is 1970-01-01 00:00:00 UTC.
	 */
	public final static ITimestamp MIN_TIME = TimestampFactory.createTimestamp(
			0L, 0L);

	/**
	 * The latest timestamp supported. This is {@link Long#MAX_VALUE} seconds
	 * and 999999999 nanoseconds since 1970-01-01 00:00:00 UTC.
	 */
	public final static ITimestamp MAX_TIME = TimestampFactory.createTimestamp(
			Long.MAX_VALUE, 999999999L);

	private static long ONE_BILLION = 1000000000L;

	/**
	 * Divides a timestamp by a number.
	 * 
	 * @param timestamp
	 *            timestamp to divide.
	 * @param divisor
	 *            number to divide by.
	 * @return divided timestamp.
	 */
	public static ITimestamp divide(ITimestamp timestamp, long divisor) {
		long seconds = timestamp.seconds();
		long nanoseconds = timestamp.nanoseconds();
		nanoseconds /= divisor;
		long remainingSeconds = seconds % divisor;
		seconds /= divisor;
		if (remainingSeconds != 0) {
			nanoseconds += remainingSeconds * ONE_BILLION / divisor;
			if (nanoseconds >= ONE_BILLION) {
				seconds += nanoseconds / ONE_BILLION;
				nanoseconds %= ONE_BILLION;
			}
		}
		return TimestampFactory.createTimestamp(seconds, nanoseconds);
	}

	/**
	 * Adds two timestamps. If one timestamp represents a point in time and the
	 * second represents a period, the resulting timestamp is the point in time
	 * at which the specified period has passed since the specified point in
	 * time. If both timestamps represents period, the resulting timestamp
	 * represents the sume of both periods.
	 * 
	 * @param timestamp1
	 *            point in time or period.
	 * @param timestamp2
	 *            period.
	 * @return point in time or period, depending on the meaning of the input
	 *         parameters.
	 */
	public static ITimestamp add(ITimestamp timestamp1, ITimestamp timestamp2) {
		long seconds = timestamp1.seconds();
		long nanoseconds = timestamp1.nanoseconds();
		seconds += timestamp2.seconds();
		nanoseconds += timestamp2.nanoseconds();
		if (nanoseconds >= ONE_BILLION) {
			seconds += nanoseconds / ONE_BILLION;
			nanoseconds %= ONE_BILLION;
		}
		return TimestampFactory.createTimestamp(seconds, nanoseconds);
	}

	/**
	 * Substracts a timestamp from another timestamp. If the first timestamp is
	 * a point in time, the result is the point in time which marks the start of
	 * the period represented by the second timestamp, that end at the point in
	 * time represented by the first timestamp. If the first timestamp
	 * represents a period, the resulting timestamp represents the period, which
	 * if added to the period represented by the second timestamp will sum up to
	 * the period represented by the first timestamp.
	 * 
	 * @param timestamp1
	 *            point in time or period, must be greater than or equal
	 *            <code>timestamp2</code>.
	 * @param timestamp2
	 *            period, must be less than or equal <code>timestamp1</code>.
	 * @return point in time or period, depending on the meaning of the input
	 *         parameters.
	 */
	public static ITimestamp substract(ITimestamp timestamp1,
			ITimestamp timestamp2) {
		if (timestamp2.isGreaterThan(timestamp1)) {
			throw new IllegalArgumentException("Cannot substract timestamp "
					+ timestamp2 + " from smaller timestamp " + timestamp1);
		}
		long seconds = timestamp1.seconds();
		long nanoseconds = timestamp1.nanoseconds();
		seconds -= timestamp2.seconds();
		nanoseconds -= timestamp2.nanoseconds();
		if (nanoseconds < 0L) {
			seconds -= 1;
			nanoseconds += ONE_BILLION;
		}
		return TimestampFactory.createTimestamp(seconds, nanoseconds);
	}
}
