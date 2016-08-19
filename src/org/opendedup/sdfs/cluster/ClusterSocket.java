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
package org.opendedup.sdfs.cluster;

import java.util.List;
import java.util.concurrent.locks.Lock;

import org.jgroups.Address;
import org.jgroups.blocks.MessageDispatcher;

public interface ClusterSocket {
	public abstract List<DSEServer> getStorageNodes();

	public abstract DSEServer getServer();

	public abstract List<DSEServer> getNameNodes();

	public abstract List<String> getVolumes();

	public abstract Address getAddressForVol(String volumeName);

	public abstract Lock getLock(String name);

	public abstract boolean isPeerMaster();

	public abstract MessageDispatcher getDispatcher();
}
