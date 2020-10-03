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

import org.opendedup.sdfs.filestore.cloud.CloudRaidStore.BucketStats;



public class SortedBucketList implements Runnable {
	private CopyOnWriteArrayList<Entry<Byte,BucketStats>> al = new CopyOnWriteArrayList<Entry<Byte,BucketStats>>();

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
	
	

	public void add(Entry<Byte,BucketStats> m) {
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

	public void remove(Entry<Byte,BucketStats> m) {
		al.remove(m);
	}

	public List<Entry<Byte,BucketStats>> getAL() {
		return this.al;
	}

	public Iterator<Entry<Byte,BucketStats>> iterator() {
		return al.iterator();
	}

	static final Comparator<Entry<Byte,BucketStats>> SIZE_ORDER = new Comparator<Entry<Byte,BucketStats>>() {
		public int compare(Entry<Byte,BucketStats> m0,
				Entry<Byte,BucketStats> m1) {
			long m1r = m1.getValue().getAvail();
			long m0r = m0.getValue().getAvail();
			long dif = m0r - m1r;
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
			byte id = 0;
			BucketStats bs = new BucketStats(id,10000,Long.valueOf(rnd.nextInt(10000)),"bn","bc");
			l.add(new java.util.AbstractMap.SimpleImmutableEntry<Byte, BucketStats>(i,bs));
		}
		l.sort();
		for(Entry<Byte,BucketStats> e : l.getAL()) {
			System.out.println(e.getValue().getAvail());
		}
		
	}
	

}
