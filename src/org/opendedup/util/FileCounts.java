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

import java.io.File;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributes;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import de.schlichtherle.truezip.file.TFile;

public class FileCounts {

	public static long getSize(File file, boolean followSymlinks)
			throws IOException {
		// Store the total size of all files
		long size = 0;
		boolean symlink = false;
		if (!OSValidator.isWindows() && !followSymlinks)
			symlink = Files.readAttributes(file.toPath(),
					PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isSymbolicLink();
		if (!symlink) {
			if (file.isDirectory()) {
				// All files and subdirectories
				File[] files = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					// Recursive call
					try {
						size += getSize(files[i], followSymlinks);
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Unable to get " + files[i].getPath(), e);
					}
				}
			}
			// Base case
			else {
				size += file.length();
			}
		}
		return size;
	}

	public static long getCounts(File file, boolean followSymlinks)
			throws IOException {
		// Store the total size of all files
		long size = 0;
		boolean symlink = false;
		if (!OSValidator.isWindows() && !followSymlinks)
			symlink = Files.readAttributes(file.toPath(),
					PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isSymbolicLink();
		if (!symlink) {
			if (file.isDirectory()) {
				// All files and subdirectories
				File[] files = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					// Recursive call
					try {
						size += getCounts(files[i], followSymlinks);
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Unable to get " + files[i].getPath(), e);
					}
				}
			}
			// Base case
			else if(!file.getPath().endsWith("mmf")){
				size = 1;
			}
		}
		return size;
	}
	
	public static long getDBFileSize(File metaFile, boolean followSymlinks)
			throws IOException {
		long size = 0;
		boolean symlink = false;
		if (!OSValidator.isWindows() && !followSymlinks)
			symlink = Files.readAttributes(metaFile.toPath(),
					PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isSymbolicLink();
		if (!symlink) {
			if (metaFile.isDirectory()) {
				// All files and subdirectories
				File[] files = metaFile.listFiles();
				for (int i = 0; i < files.length; i++) {
					// Recursive call
					try {
						size += getDBFileSize(files[i], followSymlinks);
					} catch (Exception e) {
						SDFSLogger.getLog().warn(
								"Unable to get " + files[i].getPath(), e);
					}
				}
			}
			// Base case
			else {
				MetaDataDedupFile mf = MetaDataDedupFile.getFile(metaFile
						.getPath());
				String dfGuid = mf.getDfGuid();
				if (dfGuid != null) {
					File mapFile = new File(Main.dedupDBStore + File.separator
							+ dfGuid.substring(0, 2) + File.separator + dfGuid
							+ File.separator + dfGuid + ".map");
					if(!mapFile.exists())
						mapFile = new File(Main.dedupDBStore + File.separator
								+ dfGuid.substring(0, 2) + File.separator + dfGuid
								+ File.separator + dfGuid + ".map.lz4");
					size += mapFile.length();
				}
			}
		}
		return size;
	}

	public static long getCount(File file, boolean followSymlinks)
			throws IOException {
		// Store the total size of all files
		long count = 0;
		boolean symlink = false;
		if (!OSValidator.isWindows() && !followSymlinks)
			symlink = Files.readAttributes(file.toPath(),
					PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
					.isSymbolicLink();
		if (!symlink) {
			try {
				if (file.isDirectory()) {
					// All files and subdirectories
					File[] files = file.listFiles();
					for (int i = 0; i < files.length; i++) {
						if (files[i].isFile())
							count++;
						else
							count = count + getCount(files[i], followSymlinks);
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("Unable to count " + file.getPath(), e);
			}
		}
		return count;
	}

	public static long getCount(TFile file) {
		long count = 0;
		boolean symlink = false;
		if (!symlink) {
			try {
				if (file.isDirectory()) {
					// All files and subdirectories
					TFile[] files = file.listFiles();
					for (int i = 0; i < files.length; i++) {
						if (files[i].isFile())
							count++;
						else
							count = count + getCount(files[i]);
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("Unable to count " + file.getPath(), e);
			}
		}
		return count;
	}

	public static long getSize(TFile file) {
		long size = 0;
		boolean symlink = false;
		if (!symlink) {
			try {
				if (file.isDirectory()) {
					// All files and subdirectories
					TFile[] files = file.listFiles();
					for (int i = 0; i < files.length; i++) {
						if (files[i].isFile())
							size += files[i].length();
						else
							size += getSize(files[i]);
					}
				}
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("Unable to count " + file.getPath(), e);
			}
		}
		return size;
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Size :" + getSize(new File("/home"), false));
		System.out.println("Size :" + getCount(new File("/home"), false));
	}

}
