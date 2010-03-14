/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.zipfs;

import fuse.FuseException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


public class ZipEntryDataReader
{
   private static final Log log = LogFactory.getLog(ZipEntryDataReader.class);

   private static final int bufferSize = 8192;

   private ZipFile zipFile;
   private ZipEntry zipEntry;
   private BufferedInputStream zipStream;
   private long zipPos;
   private long zipMarkPos;

   public ZipEntryDataReader(ZipFile zipFile, ZipEntry zipEntry)
   {
      this.zipFile = zipFile;
      this.zipEntry = zipEntry;
   }

   public synchronized void read(ByteBuffer bb, long offset) throws FuseException
   {
      BufferedInputStream in = getZipStream(offset, bb.capacity());

      // EOF?
      if (in == null)
         return;

      byte[] buff = new byte[bb.capacity()];
      int nread;
      try
      {
         nread = in.read(buff);
      }
      catch (IOException e)
      {
         throw new FuseException("IO error", e).initErrno(FuseException.EIO);
      }

      if (nread > 0)
      {
         zipPos += nread;
         bb.put(buff, 0, nread);
      }

      if (log.isDebugEnabled())
         log.debug("read " + bb.position() + "/" + bb.capacity() + " requested bytes");
   }


   private BufferedInputStream getZipStream(long offset, int length) throws FuseException
   {
      try
      {
         while (true)
         {
            if (zipStream == null)
            {
               zipStream = new BufferedInputStream(zipFile.getInputStream(zipEntry), bufferSize);
               zipPos = 0;
               zipStream.mark(bufferSize);
               zipMarkPos = 0;
            }

            if (offset == zipPos)
            {
               zipStream.mark(bufferSize);
               zipMarkPos = zipPos;

               return zipStream;
            }

            while (offset > zipPos)
            {
               zipStream.mark(bufferSize);
               zipMarkPos = zipPos;

               long nSkiped = zipStream.skip(offset - zipPos);
               if (nSkiped == 0)
                  return null; // premature EOF

               zipPos += nSkiped;
            }

            if (offset == zipPos)
               return zipStream;

            if (offset >= zipMarkPos)
            {
               try
               {
                  zipStream.reset();
                  zipPos = zipMarkPos;
                  continue;
               }
               catch (IOException e)
               {
                  // mark has been invalidated - can't go back
               }
            }

            // can't go back so much - reopen stream
            zipStream.close();
            zipStream = null;
         }
      }
      catch (IOException e)
      {
         throw new FuseException("IO error", e).initErrno(FuseException.EIO);
      }
   }
}

