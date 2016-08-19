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
package org.opendedup.hashing;

import java.util.Arrays;
import java.util.Random;

import com.google.common.primitives.UnsignedBytes;




public class HashTest {
	
	
	public static void main(String [] args) {
		int bs = 1024*1024;
		int runs = 10000;
		Random rnd = new Random();
		byte[] b = new byte[bs];
		rnd.nextBytes(b);
		long time = System.currentTimeMillis();
		byte [] z = null;
		for(int i = 0;i<1;i++) {
			//rnd.nextBytes(b);
			z = MurMurHash3.murmurhash3_x64_128(b, 6442);
		}
		long nt = System.currentTimeMillis() - time;
		System.out.println("took " + nt);
		time = System.currentTimeMillis();
		for(int i = 0;i<runs;i++) {
		UnsignedBytes.lexicographicalComparator().compare(z, z);
		}
		nt = System.currentTimeMillis() - time;
		System.out.println("took " + nt);
		
		time = System.currentTimeMillis();
		for(int i = 0;i<runs;i++) {
			Arrays.equals(z, z);
		}
		nt = System.currentTimeMillis() - time;
		System.out.println("took " + nt);
		time = System.currentTimeMillis();
		for(int i = 0;i<runs;i++) {
			Arrays.equals(b, b);
		}
		nt = System.currentTimeMillis() - time;
		System.out.println("took " + nt);
		
	}

}
