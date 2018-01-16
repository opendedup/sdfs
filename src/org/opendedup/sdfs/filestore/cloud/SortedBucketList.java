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
package org.opendedup.sdfs.filestore.cloud;

import java.util.Collections;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;



public class SortedBucketList implements Runnable {
	private CopyOnWriteArrayList<Entry<Byte,AtomicLong>> al = new CopyOnWriteArrayList<Entry<Byte,AtomicLong>>();

	public SortedBucketList() {
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		for (;;) {

			try {
				Thread.sleep(10 * 1000);
				synchronized(al) {
					Collections.sort(al, SIZE_ORDER);
				}

			} catch (Exception e) {

			} finally {

			}
		}
	}
	
	public void sort() {
		synchronized(al) {
			Collections.sort(al, SIZE_ORDER);
		}
	}
	
	

	public void add(Entry<Byte,AtomicLong> m) {
		try {
			al.add(m);
			Collections.reverse(al);
		} catch (Exception e) {

		} finally {

		}
	}

	public int size() {
		return this.al.size();
	}

	public void remove(Entry<Byte,AtomicLong> m) {
		al.remove(m);
	}

	public List<Entry<Byte,AtomicLong>> getAL() {
		return this.al;
	}

	public Iterator<Entry<Byte,AtomicLong>> iterator() {
		return al.iterator();
	}

	static final Comparator<Entry<Byte,AtomicLong>> SIZE_ORDER = new Comparator<Entry<Byte,AtomicLong>>() {
		public int compare(Entry<Byte,AtomicLong> m0,
				Entry<Byte,AtomicLong> m1) {
			long dif = m1.getValue().get() - m0.getValue().get();
			if (dif > 0)
				return -1;
			if (dif < 0)
				return 1;
			else
				return 0;
		}
	};
	
	
	public static void main(String [] args) {
		Random rnd = new Random();
		SortedBucketList l = new SortedBucketList();
		for(byte i = 0;i < 100;i++) {
			l.add(new java.util.AbstractMap.SimpleImmutableEntry<Byte, AtomicLong>(i,new AtomicLong(Long.valueOf(rnd.nextInt(1000)))));
		}
		l.sort();
		for(Entry<Byte,AtomicLong> e : l.getAL()) {
			System.out.println(e.getValue().get());
		}
		
	}
	

}
