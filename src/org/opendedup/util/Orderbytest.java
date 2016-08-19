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
