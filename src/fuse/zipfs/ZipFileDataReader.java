/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.zipfs;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


public class ZipFileDataReader
{
   private ZipFile zipFile;
   private Map<String, ZipEntryDataReader> zipEntry2dataReader;

   public ZipFileDataReader(ZipFile zipFile)
   {
      this.zipFile = zipFile;
      zipEntry2dataReader = new HashMap<String, ZipEntryDataReader>();
   }


   public synchronized ZipEntryDataReader getZipEntryDataReader(ZipEntry zipEntry, long offset, int size)
   {
      ZipEntryDataReader entryReader = (ZipEntryDataReader)zipEntry2dataReader.get(zipEntry.getName());

      if (entryReader == null)
      {
         entryReader = new ZipEntryDataReader(zipFile, zipEntry);
         zipEntry2dataReader.put(zipEntry.getName(), entryReader);
      }

      return entryReader;
   }

   public synchronized void releaseZipEntryDataReader(ZipEntry zipEntry)
   {
      zipEntry2dataReader.remove(zipEntry.getName());
   }
}
