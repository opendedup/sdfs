package org.rabinfingerprint.polynomial;

public class Polynomials {
	public static final long DEFAULT_POLYNOMIAL_LONG = 0x375AD14A67FC7BL;
	
	/**
	 * Generates a handful of irreducible polynomials of the specified degree.
	 */
	public static void printIrreducibles(final int degree) {
		for (int i = 0; i < 10; i++) {
			Polynomial p = Polynomial.createIrreducible(degree);
			System.out.println(p.toPolynomialString());
		}
	}

	/**
	 * Generates a large irreducible polynomial and prints out its
	 * representation in ascii and hex.
	 */
	public static void printLargeIrreducible() {
		Polynomial p = Polynomial.createIrreducible(127);
		System.out.println(p.toPolynomialString());
		System.out.println(p.toHexString());
	}

	/**
	 * Computes (a mod b) using synthetic division where a and b represent
	 * polynomials in GF(2^k).
	 */
	public static long mod(long a, long b) {
		int ma = getMaxBit(a);
		int mb = getMaxBit(b);
		for (int i = ma - mb; i >= 0; i--) {
			if (getBit(a, (i + mb))) {
				long shifted = b << i;
				a = a ^ shifted;
			}
		}
		return a;
	}

	/**
	 * Returns the index of the maximum set bit. If no bits are set, returns -1.
	 */
	public static int getMaxBit(long l) {
		for (int i = 64 - 1; i >= 0; i--) {
			if (getBit(l, i))
				return i;
		}
		return -1;
	}

	/**
	 * Returns the value of the bit at index of the long. The right most bit is
	 * at index 0.
	 */
	public static boolean getBit(long l, int index) {
		return (((l >> index) & 1) == 1);
	}

	/**
	 * Returns the value of the bit at index of the byte. The right most bit is
	 * at index 0.
	 */
	public static boolean getBit(byte b, int index) {
		return (((b >> index) & 1) == 1);
	}

	/**
	 * Returns the value of the bit at index of the byte. The right most bit is
	 * at index 0 of the last byte in the array.
	 */
	public static boolean getBit(byte[] bytes, int index) {
		// byte array index
		final int aidx = bytes.length - 1 - (index / 8);
		// bit index
		final int bidx = index % 8;
		// byte
		final byte b = bytes[aidx];
		// bit
		return getBit(b, bidx);
	}

}
