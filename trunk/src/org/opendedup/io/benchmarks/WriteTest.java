package org.opendedup.io.benchmarks;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

public class WriteTest implements Runnable {
	String path;
	int size;
	int uniqueP;
	int bs = 1048576;
	public long duration = 0;
	boolean finished = false;

	public WriteTest(String path, int size, int uniqueP) {
		this.path = path;
		this.size = size;
		this.uniqueP = uniqueP;
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			long len = 1024L * 1024L * size;
			long sz = 0;
			Path ps = Paths.get(path);
			Files.deleteIfExists(ps);
			FileChannel fc = (FileChannel) Files.newByteChannel(ps,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ);
			Random rnd = new Random();
			byte[] b = new byte[bs];
			long time = System.currentTimeMillis();
			int currPR = 0;
			while (sz < len) {
				if (currPR < this.uniqueP) {
					rnd.nextBytes(b);
				}
				ByteBuffer buf = ByteBuffer.wrap(b);
				fc.write(buf);
				sz = sz + b.length;
				if (currPR == 100)
					currPR = 0;
				else
					currPR++;
			}
			fc.force(true);
			duration = (System.currentTimeMillis() - time);
			fc.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		this.finished = true;
	}

	public float results() {
		float mb = size;
		float seconds = (duration / 1000);
		float mbps = mb / seconds;
		return mbps;
	}

	public boolean isFinished() {
		return this.finished;
	}

	public String getPath() {
		return this.path;
	}

	public void delete() throws IOException {
		Path ps = Paths.get(path);
		Files.deleteIfExists(ps);
	}

	public static float[] test(String path, int size, int unique, int runs,
			int start) {
		WriteTest[] tests = new WriteTest[runs];
		float results[] = new float[runs];
		int t = 0;
		for (int i = start; i < (start + tests.length); i++) {
			WriteTest test = new WriteTest(path + File.separator + "test" + i
					+ ".bin", size, unique);
			tests[t] = test;
			t++;
		}
		boolean finished = false;

		while (!finished) {
			int nf = 0;
			for (int i = 0; i < tests.length; i++) {
				WriteTest test = tests[i];
				if (test.isFinished()) {
					nf++;
					results[i] = test.results();
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

	public static void main(String[] args) throws IOException {
		if (args.length != 7) {
			System.out
					.println("WriteTest <Path to write to> <File Size (MB)> <precent random data (0-100)> <Number of Parallel Runs> <Number of total runs> <Test Name> <Output File>");
			System.exit(0);
		}
		int r = Integer.parseInt(args[4]);
		for (int i = 0; i < r; i++) {
			int start = i * Integer.parseInt(args[3]);
			float[] results = test(args[0], Integer.parseInt(args[1]),
					Integer.parseInt(args[2]), Integer.parseInt(args[3]), start);

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
			System.out.println("Mean= " + mean);
			float median = findMedian(results);
			System.out.println("Median= " + median);
			float mode = findMode(results);
			System.out.println("Mode= " + mode);
			float total = findTotal(results);
			System.out.println("Total= " + total);
			ch.close();
			System.out.println("Results written to " + logFileName);
		}
	}

}
