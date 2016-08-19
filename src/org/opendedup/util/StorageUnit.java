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

public enum StorageUnit {
	BYTE("B", 1L), KILOBYTE("KB", 1L << 10), MEGABYTE("MB", 1L << 20), GIGABYTE(
			"GB", 1L << 30), TERABYTE("TB", 1L << 40), PETABYTE("PB", 1L << 50), EXABYTE(
			"EB", 1L << 60);

	public static final StorageUnit BASE = BYTE;

	private final String symbol;
	private final long divider; // divider of BASE unit

	StorageUnit(String name, long divider) {
		this.symbol = name;
		this.divider = divider;
	}

	public static StorageUnit of(final double number) {
		final double n = number > 0 ? -number : number;
		if (n > -(1L << 10)) {
			return BYTE;
		} else if (n > -(1L << 20)) {
			return KILOBYTE;
		} else if (n > -(1L << 30)) {
			return MEGABYTE;
		} else if (n > -(1L << 40)) {
			return GIGABYTE;
		} else if (n > -(1L << 50)) {
			return TERABYTE;
		} else if (n > -(1L << 60)) {
			return PETABYTE;
		} else { // n >= Long.MIN_VALUE
			return EXABYTE;
		}
	}

	public static StorageUnit of(final long number) {
		final long n = number > 0 ? -number : number;
		if (n > -(1L << 10)) {
			return BYTE;
		} else if (n > -(1L << 20)) {
			return KILOBYTE;
		} else if (n > -(1L << 30)) {
			return MEGABYTE;
		} else if (n > -(1L << 40)) {
			return GIGABYTE;
		} else if (n > -(1L << 50)) {
			return TERABYTE;
		} else if (n > -(1L << 60)) {
			return PETABYTE;
		} else { // n >= Long.MIN_VALUE
			return EXABYTE;
		}
	}

	public String format(double number) {
		return nf.format(number / (double) divider) + " " + symbol;
	}

	public String format(long number) {
		return nf.format(number / (double) divider) + " " + symbol;
	}

	private static java.text.NumberFormat nf = java.text.NumberFormat
			.getInstance();
	static {
		nf.setGroupingUsed(false);
		nf.setMinimumFractionDigits(0);
		nf.setMaximumFractionDigits(2);
	}

	public static void main(String[] args) {
		long sz = Long.parseLong(args[0]);
		StorageUnit unit = StorageUnit.of(sz);
		System.out.println(unit.format(sz));
	}
}
