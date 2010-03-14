/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */
package fuse;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * The file system operations:
 *
 * Most of these should work very similarly to the well known UNIX
 * file system operations.  Exceptions are:
 *
 *  - All operations should return the error value (errno) by either:
 *     - throwing a fuse.FuseException with a errno field of the exception set to the desired fuse.Errno.E* value.
 *     - returning an integer value taken from fuse.Errno.E* constants.
 *       this is supposed to be less expensive in terms of CPU cycles and should only be used
 *       for very frequent errors (for example ENOENT).
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
 *  - flush() called when a file is closed (can be called multiple times for each dup-ed filehandle)
 *
 *  - fsync() called when file data should be synced (with a flag to sync only data but not metadata)
 *
 */
public interface Filesystem3 extends FilesystemConstants
{
   public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException;

   public int readlink(String path, CharBuffer link) throws FuseException;

   public int getdir(String path, FuseDirFiller dirFiller) throws FuseException;

   public int mknod(String path, int mode, int rdev) throws FuseException;

   public int mkdir(String path, int mode) throws FuseException;

   public int unlink(String path) throws FuseException;

   public int rmdir(String path) throws FuseException;

   public int symlink(String from, String to) throws FuseException;

   public int rename(String from, String to) throws FuseException;

   public int link(String from, String to) throws FuseException;

   public int chmod(String path, int mode) throws FuseException;

   public int chown(String path, int uid, int gid) throws FuseException;

   public int truncate(String path, long size) throws FuseException;

   public int utime(String path, int atime, int mtime) throws FuseException;

   public int statfs(FuseStatfsSetter statfsSetter) throws FuseException;

   // if open returns a filehandle by calling FuseOpenSetter.setFh() method, it will be passed to every method that supports 'fh' argument
   public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException;

   // fh is filehandle passed from open
   public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException;

   // fh is filehandle passed from open,
   // isWritepage indicates that write was caused by a writepage
   public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException;

   // called on every filehandle close, fh is filehandle passed from open
   public int flush(String path, Object fh) throws FuseException;

   // called when last filehandle is closed, fh is filehandle passed from open
   public int release(String path, Object fh, int flags) throws FuseException;

   // Synchronize file contents, fh is filehandle passed from open,
   // isDatasync indicates that only the user data should be flushed, not the meta data
   public int fsync(String path, Object fh, boolean isDatasync) throws FuseException;
}
