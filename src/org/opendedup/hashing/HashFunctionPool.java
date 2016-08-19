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
import java.security.NoSuchProviderException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class HashFunctionPool {

	private static ConcurrentLinkedQueue<AbstractHashEngine> passiveObjects = new ConcurrentLinkedQueue<AbstractHashEngine>();
	public static final String TIGER_16 = "tiger16";
	public static final String TIGER_24 = "tiger24";
	public static final String MURMUR3_16 = "murmur3_128";
	public static final String VARIABLE_MURMUR3 = "VARIABLE_MURMUR3";
	public static int hashLength = 16;
	public static int max_hash_cluster = 1;
	// public static int min_page_size = Main.CHUNK_LENGTH;
	public static int avg_page_size = 4096;

	static {
		if (Main.hashType.equalsIgnoreCase(TIGER_16)) {
			hashLength = Tiger16HashEngine.getHashLenth();
		} else if (Main.hashType.equalsIgnoreCase(MURMUR3_16)) {
			hashLength = Murmur3HashEngine.getHashLenth();
		} else if (Main.hashType.equalsIgnoreCase(VARIABLE_MURMUR3)) {
			hashLength = VariableHashEngine.getHashLenth();
			if(Main.chunkStoreLocal)
				Main.MAPVERSION = 3;
			else
				Main.MAPVERSION = 2;
			max_hash_cluster = VariableHashEngine.getMaxCluster();
		}
		SDFSLogger.getLog().info("Set hashtype to " + Main.hashType);
	}

	public static AbstractHashEngine borrowObject() throws IOException {
		AbstractHashEngine hc = null;
		hc = passiveObjects.poll();
		if (hc == null) {
			try {
				hc = makeObject();
			} catch (NoSuchAlgorithmException e) {
				throw new IOException(e);
			} catch (NoSuchProviderException e) {
				throw new IOException(e);
			}
		}
		return hc;
	}

	public static void returnObject(AbstractHashEngine hc) throws IOException {
		passiveObjects.add(hc);
	}

	public static AbstractHashEngine makeObject()
			throws NoSuchAlgorithmException, NoSuchProviderException {
		return getHashEngine();
	}

	public static void destroyObject(AbstractHashEngine hc) {
		hc.destroy();
	}

	public static AbstractHashEngine getHashEngine()
			throws NoSuchAlgorithmException, NoSuchProviderException {
		AbstractHashEngine hc = null;
		if (Main.hashType.equalsIgnoreCase(TIGER_16)) {
			hc = new Tiger16HashEngine();
		} else if (Main.hashType.equalsIgnoreCase(MURMUR3_16)) {
			hc = new Murmur3HashEngine();
		} else if (Main.hashType.equalsIgnoreCase("VARIABLE_MURMUR3")) {
			hc = new VariableHashEngine();
		}
		return hc;
	}

}
