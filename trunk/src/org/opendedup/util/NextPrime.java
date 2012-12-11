package org.opendedup.util;

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

	public static int getNextPrimeI(long input) throws Exception {
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
					throw new Exception(
							"Next Prime is Greater than max Int value");
				return (int) i;
			}
		}
	}

}