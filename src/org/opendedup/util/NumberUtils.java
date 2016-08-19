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

import java.nio.ByteBuffer;

public class NumberUtils {
	public static byte[] getBytesfromLong(long num) {
		ByteBuffer buf = ByteBuffer.wrap(new byte[8]);
		buf.putLong(num);
		return buf.array();
	}

	public static void main(String[] args) {
		int cl = 131072;
		long pos = 4096;
		int len = 4096;
		double spos = Math.ceil(((double) pos / (double) cl));
		long ep = pos + len;
		double epos = Math.floor(((double) ep / (double) cl));
		long ls = (long) spos;
		long es = (long) epos;
		if (es <= ls)
			System.out.println("eeks");
		System.out.println("spos=" + ls + " epos=" + es);
	}

}
