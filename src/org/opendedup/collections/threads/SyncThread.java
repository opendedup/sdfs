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
package org.opendedup.collections.threads;

import org.opendedup.collections.AbstractMap;

public class SyncThread implements Runnable {
	AbstractMap map;
	Thread th = null;

	public SyncThread(AbstractMap m) {
		map = m;
		th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		while (!map.isClosed()) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				break;
			}
			try {
				map.sync();
			} catch (Exception e) {
			}
		}
	}

	public void close() {
		try {
			th.interrupt();
		} catch (Exception e) {
		}
	}

}
