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
package org.opendedup.sdfs.io;

public interface VolumeListenerInterface {
	void actualWriteBytesChanged(long change, double current, Volume vol);

	void duplicateBytesChanged(long change, double current, Volume vol);

	void readBytesChanged(long change, double current, Volume vol);

	void rIOChanged(long change, double current, Volume vol);

	void wIOChanged(long change, double current, Volume vol);

	void virtualBytesWrittenChanged(long change, double current, Volume vol);

	void allowExternalSymLinksChanged(boolean symlink, Volume vol);

	void capacityChanged(long capacity, Volume vol);

	void currentSizeChanged(long capacity, Volume vol);

	void usePerMonChanged(boolean perf, Volume vol);

	void started(Volume vol);

	void mounted(Volume vol);

	void unmounted(Volume vol);

	void stopped(Volume vol);
}
