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
package org.opendedup.sdfs.io;

public interface DedupFileListener {
	void onCreate(DedupFile file);

	void onCopyTo(String dst, DedupFile file);

	void onAddLock(long position, long len, boolean shared, DedupFileLock lock,
			DedupFile file);

	void onCreateBlankFile(long size, DedupFile file);

	void onDelete(DedupFile file);

	void onForceClose(DedupFile file);

	void onRemoveHash(long position, DedupFile file);

	void onRemoveLock(DedupFileLock lock, DedupFile file);

	void onSnapShot(MetaDataDedupFile mf, DedupFile file);

	void onSync(DedupFile file);

	void onTruncate(long length, DedupFile file);

	void onUpdateMap(DedupChunkInterface writeBuffer, byte[] hash,
			boolean doop, DedupFile file);
}
