/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.FuseException;

import java.nio.ByteBuffer;


public abstract class FileNode extends Node
{
   public FileNode(String name)
   {
      super(name);
   }
   
   public abstract void open(int flags) throws FuseException;

   public abstract void release(int flags) throws FuseException;

   public abstract void read(ByteBuffer buff, long offset) throws FuseException;

   public abstract void write(ByteBuffer buff, long offset) throws FuseException;

   public abstract void truncate(long size) throws FuseException;

   public abstract void utime(int atime, int mtime) throws FuseException;
}
