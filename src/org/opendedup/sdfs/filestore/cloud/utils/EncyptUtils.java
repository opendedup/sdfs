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
package org.opendedup.sdfs.filestore.cloud.utils;

import java.io.IOException;

import org.opendedup.util.EncryptUtils;

import com.google.common.io.BaseEncoding;

public class EncyptUtils {
	public static boolean baseEncode = false;

	public static String encString(String hashes, boolean enc)
			throws IOException {
		if (baseEncode)
			return hashes;
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(hashes.getBytes());
			if (baseEncode)
				return BaseEncoding.base64().encode(encH);
			else
				return BaseEncoding.base64Url().encode(encH);

		} else {
			return BaseEncoding.base64Url().encode(hashes.getBytes());

		}
	}

	public static String decString(String fname, boolean enc)
			throws IOException {
		if (baseEncode)
			return fname;
		if (enc) {
			byte[] encH;
			if (baseEncode)
				encH = BaseEncoding.base64().decode(fname);
			else
				encH = BaseEncoding.base64Url().decode(fname);
			String st = new String(EncryptUtils.decryptCBC(encH));
			return st;
		} else {

			byte[] encH;
			encH = BaseEncoding.base64Url().decode(fname);
			return new String(encH);
		}
	}

	public static String encHashArchiveName(long id, boolean enc)
			throws IOException {
		if (baseEncode)
			return Long.toString(id);
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(Long.toString(id).getBytes());
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return BaseEncoding.base64Url()
					.encode(Long.toString(id).getBytes());
		}
	}
	
	public static String encHashArchiveName(long id,boolean useOldKey, boolean enc)
			throws IOException {
		if (baseEncode)
			return Long.toString(id);
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(Long.toString(id).getBytes(),useOldKey);
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return BaseEncoding.base64Url()
					.encode(Long.toString(id).getBytes());
		}
	}
	
	

	public static long decHashArchiveName(String fname, boolean enc)
			throws IOException {
		if (baseEncode)
			return Long.parseLong(new String(fname));
		if (enc) {
			byte[] encH;
			encH = BaseEncoding.base64Url().decode(fname);
			String st = new String(EncryptUtils.decryptCBC(encH));
			return Long.parseLong(st);
		} else {
			byte[] encH;

			encH = BaseEncoding.base64Url().decode(fname);
			return Long.parseLong(new String(encH));
		}
	}

	public static String encLong(long id, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(Long.toString(id).getBytes());
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return Long.toString(id);
		}
	}

	public static long decLong(String fname, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = BaseEncoding.base64Url().decode(fname);
			String st = new String(EncryptUtils.decryptCBC(encH));
			return Long.parseLong(st);
		} else {
			return Long.parseLong(fname);
		}
	}

	public static String encInt(int id, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(Integer.toString(id)
					.getBytes());
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return Integer.toString(id);
		}
	}

	public static int decInt(String fname, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = BaseEncoding.base64Url().decode(fname);
			String st = new String(EncryptUtils.decryptCBC(encH));
			return Integer.parseInt(st);
		} else {
			return Integer.parseInt(fname);
		}
	}

	public static String encBar(byte[] b, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = EncryptUtils.encryptCBC(b);
			return BaseEncoding.base64Url().encode(encH);
		} else {
			return BaseEncoding.base64Url().encode(b);
		}
	}

	public static byte[] decBar(String fname, boolean enc) throws IOException {
		if (enc) {
			byte[] encH = BaseEncoding.base64Url().decode(fname);
			return EncryptUtils.decryptCBC(encH);
		} else {
			return BaseEncoding.base64Url().decode(fname);
		}
	}
}