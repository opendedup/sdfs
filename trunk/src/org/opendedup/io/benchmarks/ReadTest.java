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

public class ReadTest implements Runnable {
	String path;
	int bs = 1048576;
	public long duration = 0;
	boolean finished = false;

	public ReadTest(String path) {
		this.path = path;
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			long len = new File(path).length();
			long sz = 0;
			Path ps = Paths.get(path);
			FileChannel fc = (FileChannel) Files.newByteChannel(ps,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ);
			ByteBuffer buf = ByteBuffer.allocateDirect(bs);
			long time = System.currentTimeMillis();
			while (sz < len) {
				buf.position(0);
				int read = fc.read(buf, sz);
				sz = sz + read;
			}
			duration = (System.currentTimeMillis() - time);
			fc.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		this.finished = true;
	}

	public float results() {
		long size = new File(path).length();
		float mb = (size / (1024 * 1024));
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

	public static float[] test(String path, int runs) {
		ReadTest[] tests = new ReadTest[runs];
		float results[] = new float[runs];
		for (int i = 0; i < tests.length; i++) {
			ReadTest test = new ReadTest(path + File.separator + "test" + i
					+ ".bin");
			tests[i] = test;
		}
		boolean finished = false;
		while (!finished) {
			int nf = 0;
			for (int i = 0; i < tests.length; i++) {

				ReadTest test = tests[i];
				if (test.isFinished()) {

					nf++;
					if (results[i] == 0)
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
		if (args.length != 4) {
			System.out
					.println("ReadTest <Path to read from> <Number of Parallel Runs> <Test Name> <Output File>");
			System.exit(0);
		}
		System.out.println("Running Read Test ...");
		float[] results = test(args[0], Integer.parseInt(args[1]));
		String testName = args[2];
		String logFileName = args[3];
		Path p = Paths.get(logFileName);
		boolean nf = !Files.exists(p);
		FileChannel ch = (FileChannel) Files.newByteChannel(p,
				StandardOpenOption.CREATE, StandardOpenOption.WRITE,
				StandardOpenOption.APPEND);
		if (nf) {
			String header = "test-name,date,mean (MB/s),median (MB/s),mode (MB/S),total (MB/s),sample size (GB),unique,runs\n";
			ch.write(ByteBuffer.wrap(header.getBytes()));
		}
		String output = testName + "," + new Date() + "," + findMean(results)
				+ "," + findMedian(results) + "," + findMode(results) + ","
				+ findTotal(results) + "," + args[1] + "," + args[2] + ","
				+ args[3] + "\n";
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
