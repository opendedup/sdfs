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
package org.opendedup.collections.tests;

import java.io.File;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import ec.util.MersenneTwisterFast;

import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.io.events.ArchiveSync;

import com.google.common.eventbus.EventBus;



public class DBTest implements Runnable {
	private static EventBus eventUploadBus = new EventBus();
	long size;
	int bs = 16;
	public long duration = 0;
	public static AtomicLong fn = new AtomicLong(0);
	boolean finished = false;
	private MersenneTwisterFast rnd = null;
	public static AbstractHashesMap bdb = null;

	public DBTest(long size, MersenneTwisterFast rnd) {
		this.size = size;
		this.rnd = rnd;
		Thread th = new Thread(this);
		
		th.start();
	}
	
	Exception e = null;
	
	public Exception getException() {
		return e;
		
	}
	

	@Override
	public void run() {
		try {
			long sz = 0;
			int [] seeds = new int[600];
			for(int i = 0;i<seeds.length;i++) {
				seeds[i]=rnd.nextInt();
			}
			rnd.setSeed(seeds);
			
			
			long time = System.currentTimeMillis();
			while (sz < size) {
				long arid = rnd.nextLong();
				ArrayList<byte[]> ar = new ArrayList<byte []>();
				for(int i = 0;i<1000;i++) {
					byte[] b = new byte[bs];
					rnd.nextBytes(b);
					ar.add(b);
					ChunkData cm = new ChunkData(arid, b);
					bdb.put(cm, false);
					sz++;
					
				}
				eventUploadBus.post(new ArchiveSync(ar,arid));
			}
			fn.addAndGet(sz);
			duration = (System.currentTimeMillis() - time);

		} catch (Exception e1) {
			e = e1;
		}
		this.finished = true;
	}

	public float results() {
		float mb = (float)size;
		float mbps = (mb / (float)(duration/1000));
		return mbps;
	}

	public boolean isFinished() {
		return this.finished;
	}

	

	public static float[] test(int size,int runs,
			ArrayList<MersenneTwisterFast> rnd) {
		DBTest[] tests = new DBTest[runs];
		float results[] = new float[runs];
		int t = 0;
		for (int i = 0; i < tests.length; i++) {
			DBTest test = new DBTest(size,rnd.get(t));
			tests[t] = test;
			t++;
		}
		boolean finished = false;

		while (!finished) {
			int nf = 0;
			for (int i = 0; i < tests.length; i++) {
				DBTest test = tests[i];
				if (test.isFinished()) {
					nf++;
					results[i] = test.results();
				}
				if (nf == tests.length)
					finished = true;
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return results;
	}

	public static float findMean(float array[]) {
		int sum = 0;
		int average = 0;
		int length = array.length;
		for (int j = 0; j < length; j++) {
			sum += array[j];
			average = sum / length;
		}
		return average;
	}

	// This method firstly sort the array and returns middle element if the
	// length of arrray is odd.Otherwise it will return the average of two
	// miidele elements.
	public static float findMedian(float array[]) {
		int length = array.length;
		float[] sort = new float[length];
		System.arraycopy(array, 0, sort, 0, sort.length);
		Arrays.sort(sort);

		if (length % 2 == 0) {
			return (sort[(sort.length / 2) - 1] + sort[sort.length / 2]) / 2;
		} else {
			return sort[sort.length / 2];
		}
	}

	// This method counts the occurrence of each element of array and return the
	// lement which has the maximum count.
	public static float findMode(float array[]) {
		float max = 0;
		int maxCount = 0;
		int length = array.length;
		for (int i = 0; i < length; ++i) {
			int count = 0;
			for (int j = 0; j < length; ++j) {
				if (array[j] == array[i])
					++count;
			}
			if (count > maxCount) {
				maxCount = count;
				max = array[i];
			}
		}
		return max;
	}

	public static float findTotal(float array[]) {
		float total = 0;
		int length = array.length;
		for (int i = 0; i < length; ++i) {
			total = total + array[i];
		}
		return total;
	}

	public static void main(String[] args) throws IOException, HashtableFullException {
		if (args.length != 8) {
			System.out
					.println("DBTest <dbclass> <dbdir> <inserts per thread> <Number of Parallel Runs> <Number of total runs> <Test Name> <Output File> <compact>");
			System.exit(0);
		}
		int r = Integer.parseInt(args[4]);
		Main.hashesDBClass = args[0];
		File f = new File(args[1]);
		
		f.mkdirs();
		Main.hashDBStore = f.getPath();
		connectDB();
		ShutdownHook shutdownHook = new ShutdownHook();
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		if(args[7].equalsIgnoreCase("true"))
			bdb.commitCompact(true);
		ArrayList<MersenneTwisterFast> rnds = new ArrayList<MersenneTwisterFast>(Integer.parseInt(args[3]));
		SecureRandom rnd = new SecureRandom();
		for(int i = 0;i < Integer.parseInt(args[3]);i++) {
			MersenneTwisterFast zz = new MersenneTwisterFast(rnd.nextInt());
			rnds.add(i, zz);
		}
		for (int i = 0; i < r; i++) {
			long tm = System.currentTimeMillis();
			float[] results = test(Integer.parseInt(args[2]), Integer.parseInt(args[3]), rnds);

			String testName = args[5];
			String logFileName = args[6];

			Path p = Paths.get(logFileName);
			boolean nf = !Files.exists(p);
			FileChannel ch = (FileChannel) Files.newByteChannel(p,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.APPEND);
			if (nf) {
				String header = "test-name,date,mean (MB/s),median (MB/s),mode (MB/S),total (MB/s),sample size (GB),precent unique,runs\n";
				ch.write(ByteBuffer.wrap(header.getBytes()));
			}
			String output = testName + "," + new Date() + ","
					+ findMean(results) + "," + findMedian(results) + ","
					+ findMode(results) + "," + findTotal(results) + ","
					+ args[1] + "," + args[2] + "," + args[3] + "\n";
			ch.write(ByteBuffer.wrap(output.getBytes()));
			float mean = findMean(results);
			System.out.println("Mean IOPS= " + mean);
			float median = findMedian(results);
			System.out.println("Median IOPS= " + median);
			float mode = findMode(results);
			System.out.println("Mode IOPS= " + mode);
			float total = findTotal(results);
			System.out.println("Total IOPS= " + total);
			System.out.println("Running insert total = " + fn.get());
			ch.close();
			System.out.println("insert time = " + (System.currentTimeMillis() - tm));
			System.out.println("Results written to " + logFileName + " time=" + LocalDateTime.now());
		}
		bdb.close();
	}
	
	private static void connectDB() throws IOException, HashtableFullException {
		File directory = new File(Main.hashDBStore + File.separator);
		if (!directory.exists())
			directory.mkdirs();
		File dbf = new File(directory.getPath() + File.separator + "hashstore");
		long entries = 1_000_000_000 * 200L;
		try {
			SDFSLogger.getLog().info(
					"Loading hashdb class " + Main.hashesDBClass);
			SDFSLogger.getLog().info("Maximum Number of Entries is " + entries);
			if(Main.hashesDBClass.equals("org.opendedup.collections.ShardedProgressiveFileBasedCSMap2")) {
				SDFSLogger.getLog().info("updating hashesdb class to org.opendedup.collections.RocksDBMap");
				Main.hashesDBClass = "org.opendedup.collections.RocksDBMap";
			} else if(Main.hashesDBClass.equals("backport")) {
				Main.hashesDBClass = "org.opendedup.collections.ShardedProgressiveFileBasedCSMap2";
			}
				
			bdb = (AbstractHashesMap) Class.forName(Main.hashesDBClass)
					.newInstance();
			bdb.init(entries, dbf.getPath(),Main.fpp);
			eventUploadBus.register(bdb);
		} catch (InstantiationException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			e.printStackTrace();
			System.exit(-1);
		} catch (IllegalAccessException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			e.printStackTrace();
			System.exit(-1);
		} catch (ClassNotFoundException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			e.printStackTrace();
			System.exit(-1);
		}

	}
	
	private static class ShutdownHook extends Thread {
		@Override
		public void run() {

			System.out.println("Please Wait while shutting down SDFS");
			System.out.println("Data Can be lost if this is interrupted");
			bdb.close();
			System.out.println("All Data Flushed");
			
			System.out.println("SDFS Shut Down Cleanly");

		}
	}

}
