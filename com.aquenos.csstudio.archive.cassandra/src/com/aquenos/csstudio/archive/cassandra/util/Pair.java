/*
 * Copyright 2012 aquenos GmbH.
 * All rights reserved.
 * 
 * This program and the accompanying materials are made available under the 
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html.
 */

package com.aquenos.csstudio.archive.cassandra.util;

/**
 * Groups two values of independent types, so that they can be stored or
 * returned together. This class implements the {@link #equals(Object)} and
 * {@link #hashCode()} methods, so that it can be used in collections.
 * 
 * @author Sebastian Marsching
 * 
 * @param <T1>
 *            type of the first component of pair.
 * @param <T2>
 *            type of the second component of the pair.
 */
public class Pair<T1, T2> {

	private T1 first;
	private T2 second;

	/**
	 * Constructs a pair.
	 * 
	 * @param first
	 *            value of the first component of the pair.
	 * @param second
	 *            value of the second component of the pair.
	 */
	public Pair(T1 first, T2 second) {
		this.first = first;
		this.second = second;
	}

	/**
	 * Returns the first component of the pair.
	 * 
	 * @return first component of the pair.
	 */
	public T1 getFirst() {
		return first;
	}

	/**
	 * Returns the second component of the pair.
	 * 
	 * @return second component of the pair.
	 */
	public T2 getSecond() {
		return second;
	}

	@Override
	public int hashCode() {
		int hashCode = 1;
		if (first != null) {
			hashCode *= 31;
			hashCode += first.hashCode();
		}
		if (second != null) {
			hashCode *= 31;
			hashCode += second.hashCode();
		}
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair)) {
			return false;
		}
		Pair<?, ?> other = (Pair<?, ?>) obj;
		if (this.first == null && other.first != null) {
			return false;
		}
		if (this.second == null && other.second != null) {
			return false;
		}
		if (this.first != null && !this.first.equals(other.first)) {
			return false;
		}
		if (this.second != null && !this.second.equals(other.second)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "Pair [ " + first.toString() + ", " + second.toString() + " ]";
	}

}
