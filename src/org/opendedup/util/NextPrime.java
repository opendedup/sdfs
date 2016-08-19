/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.util;

import java.io.IOException;

public class NextPrime {

	public static long getNextPrimeL(long input) {
		long root;
		boolean isPrime = false;
		if (input <= 2) {
			return 2;
		}
		for (int k = 3; k < 9; k += 2) {
			if (input <= k) {
				return k;
			}
		}
		if (input == ((input >> 1) << 1)) {
			input += 1;
		}
		for (long i = input;; i += 2) {
			root = (long) Math.sqrt(i);
			for (long j = 3; j <= root; j++) {
				if (i == (i / j) * j) {
					isPrime = false;
					break;
				}
				if (j == root) {
					isPrime = true;
				}
			}
			if (isPrime == true) {
				return i;
			}
		}
	}

	public static int getNextPrimeI(long input) throws IOException {
		long root;
		boolean isPrime = false;
		if (input <= 2) {
			return 2;
		}
		for (int k = 3; k < 9; k += 2) {
			if (input <= k) {
				return k;
			}
		}
		if (input == ((input >> 1) << 1)) {
			input += 1;
		}
		for (long i = input;; i += 2) {
			root = (long) Math.sqrt(i);
			for (long j = 3; j <= root; j++) {
				if (i == (i / j) * j) {
					isPrime = false;
					break;
				}
				if (j == root) {
					isPrime = true;
				}
			}
			if (isPrime == true) {
				if (i > Integer.MAX_VALUE)
					throw new IOException(
							"Next Prime is Greater than max Int value");
				return (int) i;
			}
		}
	}

}