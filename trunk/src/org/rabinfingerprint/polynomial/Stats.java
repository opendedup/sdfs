package org.rabinfingerprint.polynomial;

public class Stats {
	private long count = 0L;
	private double sum = 0.0;

	public synchronized void add(double value) {
		sum += value;
		count++;
	}

	public synchronized double average() {
		if (count == 0) {
			return 0;
		}
		return sum / count;
	}

	public synchronized double sum() {
		return sum;
	}

	public synchronized long count() {
		return count;
	}
}