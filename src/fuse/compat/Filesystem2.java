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
 * The file system operations:
 *
 * Most of these should work very similarly to the well known UNIX
 * file system operations.  Exceptions are:
 *
 *  - All operations should return the error value (errno) by throwing a
 *  fuse.FuseException with a errno field of the exception set to the desired value.
 *
 *  - getdir() is the opendir(), readdir(), ..., closedir() sequence
 *  in one call.
 *
 *  - There is no create() operation, mknod() will be called for
 *  creation of all non directory, non symlink nodes.
 *
 *  - open() No
 *  creation, or trunctation flags (O_CREAT, O_EXCL, O_TRUNC) will be
 *  passed to open().  Open should only check if the operation is
 *  permitted for the given flags.
 *
 *  - read(), write(), release() are are passed a filehandle that is returned from open() in
 *  addition to a pathname.  The offset of the read and write is passed as the last
 *  argument, the number of bytes read/writen is returned through the java.nio.ByteBuffer object
 *
 *  - release() is called when an open file has:
 *       1) all file descriptors closed
 *       2) all memory mappings unmapped
 *    This call need only be implemented if this information is required.
 *
 * New operations in FUSE-J 2.2.1:
 *
 *  - flush() called when a file is closed (can be called multiple times for each dup-ed filehandle)
 *
 *  - fsync() called when file data should be synced (with a flag to sync only data but not metadata)
 *
 */
public interface Filesystem2 extends FilesystemConstants
{
   public FuseStat getattr(String path) throws FuseException;

   public String readlink(String path) throws FuseException;

   // CHANGE-22: FuseDirEnt.inode added
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

   // CHANGE-22: if open returns a filehandle it is passed to every other filesystem call
   public long open(String path, int flags) throws FuseException;

   // CHANGE-22: fh is filehandle passed from open
   public void read(String path, long fh, ByteBuffer buf, long offset) throws FuseException;

   // CHANGE-22: fh is filehandle passed from open,
   //            isWritepage indicates that write was caused by a writepage
   public void write(String path, long fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException;

   // CHANGE-22: new operation (called on every filehandle close), fh is filehandle passed from open
   public void flush(String path, long fh) throws FuseException;

   // CHANGE-22: (called when last filehandle is closed), fh is filehandle passed from open
   public void release(String path, long fh, int flags) throws FuseException;

   // CHANGE-22: new operation (Synchronize file contents), fh is filehandle passed from open,
   //            isDatasync indicates that only the user data should be flushed, not the meta data
   public void fsync(String path, long fh, boolean isDatasync) throws FuseException;
}
