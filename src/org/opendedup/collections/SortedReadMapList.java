package org.opendedup.collections;

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
				Collections.sort(al,TIME_ORDER);
				
			} catch (Exception e) {

			} finally {

			}
		}
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
			long dif = m0.lastFound - m1.lastFound;
			if (dif > 0)
				return -1;
			if (dif < 0)
				return 1;
			else
				return 0;
		}
	};

}
