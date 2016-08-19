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
package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HashExistsCmd implements IOCmd {
	byte[] hash;
	boolean exists = false;

	public HashExistsCmd(byte[] hash) {
		this.hash = hash;
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.HASH_EXISTS_CMD);
		os.writeShort(hash.length);
		os.write(hash);
		os.flush();
		this.exists = is.readBoolean();
	}

	public byte[] getHash() {
		return this.hash;
	}

	public boolean exists() {
		return this.exists;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.HASH_EXISTS_CMD;
	}

	@Override
	public Boolean getResult() {
		return this.exists;
	}

}
