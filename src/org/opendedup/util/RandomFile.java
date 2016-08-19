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
package org.opendedup.util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.opendedup.sdfs.Main;

public class RandomFile {
	public static void writeRandomFile(String fileName, double size)
			throws IOException {
		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(fileName));
		long currentpos = 0;
		Random r = new Random();
		while (currentpos < size) {
			byte[] rndB = new byte[Main.CHUNK_LENGTH];
			r.nextBytes(rndB);
			out.write(rndB);
			currentpos = currentpos + rndB.length;
			out.flush();
		}
		out.flush();
		out.close();
	}

	public static void main(String[] args) throws IOException {
		long size = 100 * 1024L * 1024L * 1024L;
		writeRandomFile("/media/dedup/rnd.bin", size);
	}

}
