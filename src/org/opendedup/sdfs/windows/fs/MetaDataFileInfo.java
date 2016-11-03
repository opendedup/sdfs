

package org.opendedup.sdfs.windows.fs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.DosFileAttributes;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.FileAttribute.FileAttributeFlags;
import net.decasdev.dokan.Win32FindData;

import org.apache.commons.io.FilenameUtils;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MetaDataFileInfo {
	static long nextFileIndex = 2;

	String fileName;
	final boolean isDirectory;
	private int fileAttribute = 0;
	long creationTime = 0;
	long lastAccessTime = 0;
	long lastWriteTime = 0;
	final long fileIndex;
	final long fileSize;
	MetaDataDedupFile mf = null;

	MetaDataFileInfo(String fileName, MetaDataDedupFile mf) {
		this.fileName = fileName;
		this.mf = mf;
		this.isDirectory = mf.isDirectory();
		fileIndex = getNextFileIndex();
		Path file = Paths.get(mf.getPath());
		try {
			DosFileAttributes attr = Files.readAttributes(file,
					DosFileAttributes.class);
			if (attr.isArchive())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_ARCHIVE
						.getValue();
			if (attr.isDirectory())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_DIRECTORY
						.getValue();
			if (attr.isHidden())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_HIDDEN
						.getValue();
			if (attr.isReadOnly())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_READONLY
						.getValue();
			if (attr.isRegularFile())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_NORMAL
						.getValue();
			if (attr.isSymbolicLink())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_REPARSE_POINT
						.getValue();
			if (attr.isSystem())
				fileAttribute |= FileAttributeFlags.FILE_ATTRIBUTE_SYSTEM
						.getValue();
			creationTime = millisToFiletime(attr.creationTime().toMillis());
			lastAccessTime = millisToFiletime(mf.getLastAccessed());
			//SDFSLogger.getLog().info("fn="+this.fileName+" mtime=" +mf.lastModified() + " ft="+ attr.lastModifiedTime().toMillis() + " wt=" +millisToFiletime(mf.lastModified()));
			lastWriteTime = millisToFiletime(mf.lastModified());
		} catch (IOException | UnsupportedOperationException x) {
			SDFSLogger.getLog().error(
					"attributes could not be created for " + this.fileName, x);
		}

		
		fileSize = mf.length();
		SDFSLogger.getLog().debug("created file info for " + fileName);
	}
	
	/** Difference between Filetime epoch and Unix epoch (in ms). */
	private static final long FILETIME_EPOCH_DIFF = 11644473600000L;

	/** One millisecond expressed in units of 100s of nanoseconds. */
	private static final long FILETIME_ONE_MILLISECOND = 10 * 1000;

	public static long filetimeToMillis(final long filetime) {
		if(filetime <= 0)
			return 0;
	    return (filetime / FILETIME_ONE_MILLISECOND) - FILETIME_EPOCH_DIFF;
	}

	public static long millisToFiletime(final long millis) {
	    return (millis + FILETIME_EPOCH_DIFF) * FILETIME_ONE_MILLISECOND;
	}

	Win32FindData toWin32FindData() {
		String lName = FilenameUtils.getName(fileName);
		String sName = Utils.toShortName(fileName);
		Win32FindData d = new Win32FindData(fileAttribute, creationTime,
				lastAccessTime, lastWriteTime, fileSize, 0, 0, lName, sName);
		return d;
	}

	ByHandleFileInformation toByHandleFileInformation() {
		return new ByHandleFileInformation(fileAttribute, creationTime,
				lastAccessTime, lastWriteTime, (int)(Main.volume.getSerialNumber() >> 32),
				fileSize, 1, fileIndex);
	}

	static synchronized long getNextFileIndex() {
		if (nextFileIndex == Long.MAX_VALUE)
			nextFileIndex = 0;
		return nextFileIndex++;
	}
	public String toString() {
		return "lName = " + fileName + " fileAttribure = " + this.fileAttribute + " ctime = " + 
		this.creationTime + " lastAccessTime = " + this.lastAccessTime +  " lastWriteTime =" + this.lastWriteTime +
		" fileSize = " + this.fileSize + " fileInzed = " + this.fileIndex;
	}
}