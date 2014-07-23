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
