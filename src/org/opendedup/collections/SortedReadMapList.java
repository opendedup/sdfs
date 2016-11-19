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
package org.opendedup.collections;

import java.util.ArrayList;
import java.util.Collections;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SortedReadMapList implements Runnable {
	private CopyOnWriteArrayList<ProgressiveFileByteArrayLongMap> al = new CopyOnWriteArrayList<ProgressiveFileByteArrayLongMap>();

	public SortedReadMapList() {
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		for (;;) {

			try {
				Thread.sleep(10 * 1000);
				synchronized(al) {
					Collections.sort(al, TIME_ORDER);
				}

			} catch (Exception e) {

			} finally {

			}
		}
	}
	
	public void sort() {
		synchronized(al) {
			Collections.sort(al, TIME_ORDER);
		}
	}
	
	public List<ProgressiveFileByteArrayLongMap> getLMMap() {
		ArrayList<ProgressiveFileByteArrayLongMap> _al = new ArrayList<ProgressiveFileByteArrayLongMap>();
		synchronized(al) {
		for(ProgressiveFileByteArrayLongMap m : al) {
			_al.add(m);
		}
		}
		Collections.sort(_al, LM_ORDER);
		return _al;
	}

	public void add(ProgressiveFileByteArrayLongMap m) {
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

	public void remove(ProgressiveFileByteArrayLongMap m) {
		al.remove(m);
	}

	public List<ProgressiveFileByteArrayLongMap> getAL() {
		return this.al;
	}

	public Iterator<ProgressiveFileByteArrayLongMap> iterator() {
		return al.iterator();
	}

	static final Comparator<ProgressiveFileByteArrayLongMap> TIME_ORDER = new Comparator<ProgressiveFileByteArrayLongMap>() {
		public int compare(ProgressiveFileByteArrayLongMap m0,
				ProgressiveFileByteArrayLongMap m1) {
			long dif = m0.getLastAccess() - m1.getLastAccess();
			if (dif > 0)
				return -1;
			if (dif < 0)
				return 1;
			else
				return 0;
		}
	};
	
	static final Comparator<ProgressiveFileByteArrayLongMap> LM_ORDER = new Comparator<ProgressiveFileByteArrayLongMap>() {
		public int compare(ProgressiveFileByteArrayLongMap m0,
				ProgressiveFileByteArrayLongMap m1) {
			long dif = m0.getLastModified() - m1.getLastModified();
			if (dif > 0)
				return -1;
			if (dif < 0)
				return 1;
			else
				return 0;
		}
	};

}
