/*
The MIT License

Copyright (C) 2008 Yu Kobayashi http://yukoba.accelart.jp/

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */

package org.opendedup.sdfs.windows.fs;

import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_DIRECTORY;

import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_NORMAL;

import java.util.Date;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;

import org.apache.commons.io.FilenameUtils;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MetaDataFileInfo {
	static long nextFileIndex = 2;

	String fileName;
	final boolean isDirectory;
	private int fileAttribute = FILE_ATTRIBUTE_NORMAL;
	long creationTime = 0;
	long lastAccessTime = 0;
	long lastWriteTime = 0;
	final long fileIndex;
	MetaDataDedupFile mf = null;

	MetaDataFileInfo(String fileName, MetaDataDedupFile mf) {
		this.fileName = fileName;
		this.mf = mf;
		this.isDirectory = mf.isDirectory();
		fileIndex = getNextFileIndex();
		if (isDirectory)
			fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;
		creationTime = FileTimeUtils.toFileTime(new Date(mf.getTimeStamp()));
		lastAccessTime = FileTimeUtils
				.toFileTime(new Date(mf.getLastAccessed()));
		lastWriteTime = FileTimeUtils.toFileTime(new Date(mf.lastModified()));
		SDFSLogger.getLog().debug("created file info for " + fileName);
	}

	Win32FindData toWin32FindData() {
		String lName = FilenameUtils.getName(fileName);
		String sName = Utils.toShortName(fileName);
		Win32FindData d = new Win32FindData(fileAttribute, creationTime,
				lastAccessTime, lastWriteTime, getFileSize(), 0, 0, lName,
				sName);
		return d;
	}

	ByHandleFileInformation toByHandleFileInformation() {
		return new ByHandleFileInformation(fileAttribute, creationTime,
				lastAccessTime, lastWriteTime, WinSDFS.volumeSerialNumber,
				getFileSize(), 1, fileIndex);
	}

	long getFileSize() {
		return mf.length();
	}

	static long getNextFileIndex() {
		if (nextFileIndex == Long.MAX_VALUE)
			nextFileIndex = 0;
		return nextFileIndex++;
	}
}
