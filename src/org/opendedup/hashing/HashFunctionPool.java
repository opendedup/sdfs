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
	public static final String VARIABLE_SIP = "VARIABLE_SIP";
	public static final String VARIABLE_SIP2 = "VARIABLE_SIP2";
	public static final String VARIABLE_SHA256 = "VARIABLE_SHA256";
	public static final String VARIABLE_SHA256_160 = "VARIABLE_SHA256_160";
	public static final String VARIABLE_HWY_160 = "VARIABLE_HWY_160";
	public static final String VARIABLE_HWY_128 = "VARIABLE_HWY_128";
	public static final String VARIABLE_HWY_256 = "VARIABLE_HWY_256";
	public static final String VARIABLE_MD5 = "VARIABLE_MD5";
	public static int hashLength = 16;
	public static int max_hash_cluster = 1;
	// public static int min_page_size = Main.CHUNK_LENGTH;
	public static int avg_page_size = 4096;
	public static int minLen = Main.MIN_CHUNK_LENGTH;
	public static int maxLen = Main.CHUNK_LENGTH;
	public static long bytesPerWindow = 48;

	static {
		if (Main.hashType.equalsIgnoreCase(TIGER_16)) {
			hashLength = Tiger16HashEngine.getHashLenth();
		} else if (Main.hashType.equalsIgnoreCase(MURMUR3_16)) {
			hashLength = Murmur3HashEngine.getHashLenth();
		} else if (Main.hashType.toUpperCase().startsWith("VARIABLE_")) {
			if(Main.hashType.endsWith("256")) {
				hashLength = 32;
			}
			else if(Main.hashType.endsWith("160")) {
				hashLength = 18;
			}
			else if(Main.hashType.endsWith("128")) {
				hashLength = 16;
			}else
				hashLength = 16;
			Main.MAPVERSION = 3;
			max_hash_cluster = Main.CHUNK_LENGTH / HashFunctionPool.minLen;
		}
		SDFSLogger.getLog().info("Set hashtype to " + Main.hashType + " hash length = " + hashLength + " maxhashcluster= " + max_hash_cluster + " chunk-length=" + Main.CHUNK_LENGTH + " minlen=" + minLen);
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
		SDFSLogger.getLog().debug("Size="+passiveObjects.size());
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
			 {
		try {
		AbstractHashEngine hc = null;
		if (Main.hashType.equalsIgnoreCase(TIGER_16)) {
			hc = new Tiger16HashEngine();
		} else if (Main.hashType.equalsIgnoreCase(MURMUR3_16)) {
			hc = new Murmur3HashEngine();
		} else if (Main.hashType.equalsIgnoreCase("VARIABLE_MURMUR3")) {
			hc = new VariableHashEngine();
		}
		else if (Main.hashType.equalsIgnoreCase("VARIABLE_SIP") || Main.hashType.equalsIgnoreCase("VARIABLE_SIP2")) {
			hc = new VariableMD5HashEngine();
		}
		else if (Main.hashType.equalsIgnoreCase(VARIABLE_SHA256)) {
			hc = new VariableSha256HashEngine(VariableSha256HashEngine.HASHTYPE.HASH256);
		}
		else if (Main.hashType.equalsIgnoreCase(VARIABLE_SHA256_160)) {
			hc = new VariableSha256HashEngine(VariableSha256HashEngine.HASHTYPE.HASH160);
		}
		else if (Main.hashType.equalsIgnoreCase(VARIABLE_HWY_128)) {
			hc = new VariableHighwayHashEngine(VariableHighwayHashEngine.HASHTYPE.HASH128);
		}
		else if (Main.hashType.equalsIgnoreCase(VARIABLE_HWY_160)) {
			hc = new VariableHighwayHashEngine(VariableHighwayHashEngine.HASHTYPE.HASH160);
		}
		else if (Main.hashType.equalsIgnoreCase(VARIABLE_HWY_256)) {
			hc = new VariableHighwayHashEngine(VariableHighwayHashEngine.HASHTYPE.HASH256);
		}
		else if (Main.hashType.equalsIgnoreCase(VARIABLE_MD5)) {
			hc = new VariableMD5HashEngine();
		}
		return hc;
		}catch(Exception e) {
			SDFSLogger.getLog().fatal("unable to get engine", e);
			System.exit(5);
			return null;
		}
	}

}
