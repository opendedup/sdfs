package org.opendedup.util;

import java.util.ArrayList;

public class FactorTest {
	public static void main(String[] args) {
		int val = 128 * 1024;

		System.out.println("\nThe factors of " + val + " are:");
		int[] result = factorsOf(val);
		for (int i = 0; i < result.length && result[i] != 0; i++) {
			System.out.println(result[i]);
		}
		System.out.println("closest=" + result[closest2Pos(3122, result)]);
	}

	public static int[] factorsOf(double val) {
		ArrayList<Integer> al = new ArrayList<Integer>();
		double prev = val;
		al.add((int)val);
		while (val >= 512) {
			if((prev -val) >= 512) {
				al.add((int)val);
				prev = val;
			}
			val = val / 1.2;
		}
		int[] z = new int[al.size()];
		for (int i = 0; i < al.size(); i++) {
			z[i] = al.get(i);
		}
		return z;
	}

	public static int closest2(int find, int[] values) {
		int distance = Integer.MAX_VALUE;
		int closest = -1;
		for (int i : values) {
			int distanceI = i - find;
			if (distanceI > -1 && distance >= distanceI) {
				distance = distanceI;
				closest = i;
			}
		}
		return closest;
	}

	public static int closest2Pos(int find, int[] values) {
		int distance = Integer.MAX_VALUE;
		int closest = -1;
		for (int z = 0; z < values.length; z++) {
			int i = values[z];
			int distanceI = i - find;
			if (distanceI > -1 && distance >= distanceI) {
				distance = distanceI;
				closest = z;
			}
		}
		return closest;
	}

}
