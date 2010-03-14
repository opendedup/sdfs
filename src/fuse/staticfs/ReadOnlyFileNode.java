/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.FuseFS;
import fuse.FuseException;
import fuse.FuseFtype;
import fuse.compat.FuseStat;

import java.nio.ByteBuffer;


public class ReadOnlyFileNode extends FileNode
{
   private byte[] content;

   public ReadOnlyFileNode(String name)
   {
      this(name, new byte[0]);
   }

   public ReadOnlyFileNode(String name, byte[] content)
   {
      super(name);

      setContent(content);
   }

   //
   // create initial FuseStat structure (called from Node's constructor)

   protected FuseStat createStat()
   {
      FuseStat stat = new FuseStat();

      stat.mode = FuseFtype.TYPE_FILE | 0444;
      stat.uid = stat.gid = 0;
      stat.ctime = stat.mtime = stat.atime = (int)(System.currentTimeMillis() / 1000L);
      stat.size = 0;
      stat.blocks = 0;

      return stat;
   }

   //
   // FileNode implementation

   public synchronized void read(ByteBuffer buff, long offset) throws FuseException
   {
      if (offset >= content.length)
         return;

      int length = buff.capacity();
      if (offset + length > content.length)
         length = content.length - (int)offset;

      buff.put(content, (int)offset, length);
   }

   public void write(ByteBuffer buff, long offset) throws FuseException
   {
      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void open(int flags) throws FuseException
   {
      if (flags == FuseFS.O_RDWR || flags == FuseFS.O_WRONLY)
         throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void release(int flags) throws FuseException
   {
      // noop
   }

   public void truncate(long size) throws FuseException
   {
      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void utime(int atime, int mtime) throws FuseException
   {
      // noop
   }

   //
   // file content access

   public synchronized byte[] getContent()
   {
      return content;
   }

   public synchronized void setContent(byte[] content)
   {
      // stat is by declaration read-only - we must create a copy before modifying it's attributes
      FuseStat stat = (FuseStat) super.getStat().clone();

      if (this.content == null)
         stat.ctime = (int)(System.currentTimeMillis() / 1000L);

      this.content = content;

      stat.mtime = stat.atime = (int)(System.currentTimeMillis() / 1000L);
      stat.size = content.length;
      stat.blocks = (content.length + 511) / 512;

      super.setStat(stat);
   }
}
