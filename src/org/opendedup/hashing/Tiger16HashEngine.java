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
package org.opendedup.hashing;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.opendedup.sdfs.Main;

import jonelo.jacksum.JacksumAPI;
import jonelo.jacksum.algorithm.AbstractChecksum;

public class Tiger16HashEngine implements AbstractHashEngine {

	AbstractChecksum md = null;

	public Tiger16HashEngine() throws NoSuchAlgorithmException {
		md = JacksumAPI.getChecksumInstance("tiger128");
	}

	@Override
	public byte[] getHash(byte[] data) {
		md.update(data);
		byte[] hash = md.getByteArray();
		md.reset();
		return hash;
	}

	public static int getHashLenth() {
		// TODO Auto-generated method stub
		return 16;
	}

	@Override
	public void destroy() {
		md.reset();
		md = null;
	}

	@Override
	public boolean isVariableLength() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getMaxLen() {
		// TODO Auto-generated method stub
		return Main.CHUNK_LENGTH;
	}

	@Override
	public int getMinLen() {
		// TODO Auto-generated method stub
		return Main.CHUNK_LENGTH;
	}

	@Override
	public void setSeed(int seed) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public List<Finger> getChunks(byte [] b,String lp,String uuid) throws IOException {
		throw new IOException("not supported");
	}
}
