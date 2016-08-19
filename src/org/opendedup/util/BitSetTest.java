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
import java.util.Random;

import org.apache.lucene.util.OpenBitSet;

public class BitSetTest {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		OpenBitSet set = new OpenBitSet(18719476739L);
		// long bv = (Long.MAX_VALUE/2)+4;
		// set.set((Long.MAX_VALUE/2)+4, true);
		// System.out.println("bv=" + bv + " lv=" +set.nextSetBit(0));
		Random r = new Random();
		long smallest = Long.MAX_VALUE;
		long tm = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			long nv = (long) (r.nextDouble() * (18719476739L));
			if (nv < 0)
				nv = nv * -1;
			if (nv < 18719476736L) {
				if (nv < smallest)
					smallest = nv;
				set.fastSet(nv);
				if (!set.get(nv))
					System.out.println("failed at " + nv);
			}
		}
		long dur = System.currentTimeMillis() - tm;
		System.out.println("duration=" + dur);
		System.out.println("Size=" + set.cardinality());
		long sm = set.nextSetBit(0);
		System.out.println("smallest=" + smallest + " sm=" + sm);

		OpenBitSetSerialize.writeOut("/tmp/test.bin", set);
	}

}
