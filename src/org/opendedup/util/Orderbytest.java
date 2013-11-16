package org.opendedup.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

public class Orderbytest {
	public static void main(String[] args) {
		ArrayList<Long> al = new ArrayList<Long>();
		Random r = new Random();
		for (int i = 0; i < 15; i++) {
			al.add(new Long(r.nextLong()));
		}
		Collections.sort(al, new CustomComparator());
		for (int i = 0; i < al.size(); i++) {
			System.out.println(al.get(i));
		}
	}

	private static class CustomComparator implements Comparator<Long> {
		@Override
		public int compare(Long o1, Long o2) {

			return o1.compareTo(o2);
		}
	}

}
