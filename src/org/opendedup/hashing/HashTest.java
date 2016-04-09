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
