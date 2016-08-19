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
package org.opendedup.sdfs.monitor;

public interface IOMonitorListener {
	void actualBytesWrittenChanged(long total, int change, IOMonitor mon);

	void bytesReadChanged(long total, int change, IOMonitor mon);

	void duplicateBlockChanged(long total, IOMonitor mon);

	void rioChanged(long total, IOMonitor mon);

	void virtualBytesWrittenChanged(long total, int change, IOMonitor mon);

	void wioChanged(long total, IOMonitor mon);

	void clearAllCountersExecuted(long total, IOMonitor mon);

	void clearFileCountersExecuted(long total, IOMonitor mon);

	void removeDuplicateBlockChanged(long total, IOMonitor mon);

	void actualBytesWrittenChanged(long total, long change, IOMonitor mon);

	void bytesReadChanged(long total, long change, IOMonitor mon);

	void duplicateBlockChanged(long total, long change, IOMonitor mon);

	void virtualBytesWrittenChanged(long total, long change, IOMonitor mon);

	void riopsChanged(int iops, int changed, IOMonitor mon);

	void wiopsChanged(int iops, int changed, IOMonitor mon);

	void iopsChanged(int iops, int changed, IOMonitor mon);

	void rmbpsChanged(long mbps, int changed, IOMonitor mon);

	void wmbpsChanged(long mbps, int changed, IOMonitor mon);

	void mbpsChanged(long mbps, int changed, IOMonitor mon);

	void qosChanged(int old, int newQos, IOMonitor mon);

	void ioProfileChanged(String old, String newProf, IOMonitor mon);
}
