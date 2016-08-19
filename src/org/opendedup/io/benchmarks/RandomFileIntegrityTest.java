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
package org.opendedup.io.benchmarks;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Files;

public class RandomFileIntegrityTest implements Runnable {
	File path;
	int size;
	boolean finished = false;
	boolean passed = false;
	String hashcode = null;

	public RandomFileIntegrityTest(File path, int size) {
		this.path = path;
		this.size = size;
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			int len = 1024 * size;

			Random rnd = new Random();
			byte[] b = new byte[len];
			rnd.nextBytes(b);
			HashFunction hf = Hashing.murmur3_128();
			byte[] nhc = hf.hashBytes(b).asBytes();
			hashcode = BaseEncoding.base16().encode(nhc);
			path = new File(path.getPath() + File.separator + hashcode);
			Files.write(b, path);
			byte[] hc = Files.hash(path, Hashing.murmur3_128()).asBytes();
			passed = Arrays.equals(hc, nhc);
			hashcode = BaseEncoding.base16().encode(hc);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.finished = true;
	}

	public boolean isFinished() {
		return this.finished;
	}

	public File getPath() {
		return this.path;
	}

	public static int test(String path, int size, int runs) throws IOException {
		RandomFileIntegrityTest[] tests = new RandomFileIntegrityTest[runs];
		for (int i = 0; i < tests.length; i++) {
			RandomFileIntegrityTest test = new RandomFileIntegrityTest(
					new File(path), size);
			tests[i] = test;
		}
		boolean finished = false;
		int passed = 0;
		;
		while (!finished) {
			int nf = 0;
			for (int i = 0; i < tests.length; i++) {
				RandomFileIntegrityTest test = tests[i];
				if (test.isFinished()) {
					nf++;
					if (test.passed)
						passed++;
				}
				if (nf == tests.length)
					finished = true;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return passed;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (args.length != 4) {
			System.out
					.println("RandomFileIntegrityTest <path to write to> <File Size (KB)> <Number of Parallel Runs> <Number of total runs>");
			System.exit(0);
		}
		int r = Integer.parseInt(args[3]);
		for (int i = 0; i < r; i++) {
			test(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
		}
		Process p = Runtime.getRuntime().exec("sync");
		p.waitFor();
		p = Runtime.getRuntime().exec("echo 3 > /proc/sys/vm/drop_caches");
		p.waitFor();
		File f = new File(args[0]);
		File[] fs = f.listFiles();
		System.out.println("Checking " + fs.length);
		int passed = 0;
		for (File hf : fs) {
			byte[] hc = BaseEncoding.base16().decode(hf.getName());
			byte[] nhc = Files.hash(hf, Hashing.murmur3_128()).asBytes();
			if (Arrays.equals(hc, nhc)) {
				passed++;
			}
		}
		System.out.println("Files=" + fs.length + " Passed=" + passed);
	}

}
