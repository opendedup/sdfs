/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */
package fuse.compat;

import fuse.*;

import java.nio.ByteBuffer;

/**
 * This is an old compatibility API (renamed from fuse.Filesystem)
 * Use fuse.Filesystem instead for new applications
 */
public interface Filesystem1 extends FilesystemConstants
{
   public FuseStat getattr(String path) throws FuseException;

   public String readlink(String path) throws FuseException;

   public FuseDirEnt[] getdir(String path) throws FuseException;

   public void mknod(String path, int mode, int rdev) throws FuseException;

   public void mkdir(String path, int mode) throws FuseException;

   public void unlink(String path) throws FuseException;

   public void rmdir(String path) throws FuseException;

   public void symlink(String from, String to) throws FuseException;

   public void rename(String from, String to) throws FuseException;

   public void link(String from, String to) throws FuseException;

   public void chmod(String path, int mode) throws FuseException;

   public void chown(String path, int uid, int gid) throws FuseException;

   public void truncate(String path, long size) throws FuseException;

   public void utime(String path, int atime, int mtime) throws FuseException;

   public FuseStatfs statfs() throws FuseException;

   public void open(String path, int flags) throws FuseException;

   public void read(String path, ByteBuffer buf, long offset) throws FuseException;

   public void write(String path, ByteBuffer buf, long offset) throws FuseException;

   public void release(String path, int flags) throws FuseException;
}
