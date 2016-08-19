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
package org.opendedup.sdfs.servers;

public class HCServer {
	String hostName;
	int port;
	boolean useUDP;
	boolean compress;
	boolean useSSL;

	public HCServer(String hostName, int port, boolean useUDP,
			boolean compress, boolean useSSL) {
		this.hostName = hostName;
		this.port = port;
		this.useUDP = useUDP;
		this.compress = compress;
		this.useSSL = useSSL;
	}

	public boolean isCompress() {
		return compress;
	}

	public boolean isUseUDP() {
		return useUDP;
	}

	public String getHostName() {
		return hostName;
	}

	public int getPort() {
		return port;
	}

	public boolean isSSL() {
		return this.useSSL;
	}

}
