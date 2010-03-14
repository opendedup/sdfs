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


/**
 * This is a byte level filesystem API (in contrast to String level filesystem API like fuse.Filesystem[123]).
 * Any paths/names are passed as native ByteBuffer objects or byte[] arrays. This is the interface
 * that is called from JNI bindings. It is not intended that this interface be implemented directly by users
 * but instead a fuse.Filesystem[123] interface should be implemented and encoding of file names and paths should
 * be left to a special adapter class: fuse.Filesystem3ToFuseFSAdapter.
 *
 * Return value from every method is allways 0 for success or errno for error
 */
public interface FuseFS extends FilesystemConstants
{
   public int getattr(ByteBuffer path, FuseGetattrSetter getattrSetter);

   public int readlink(ByteBuffer path, ByteBuffer link);

   public int getdir(ByteBuffer path, FuseFSDirFiller dirFiller);

   public int mknod(ByteBuffer path, int mode, int rdev);

   public int mkdir(ByteBuffer path, int mode);

   public int unlink(ByteBuffer path);

   public int rmdir(ByteBuffer path);

   public int symlink(ByteBuffer from, ByteBuffer to);

   public int rename(ByteBuffer from, ByteBuffer to);

   public int link(ByteBuffer from, ByteBuffer to);

   public int chmod(ByteBuffer path, int mode);

   public int chown(ByteBuffer path, int uid, int gid);

   public int truncate(ByteBuffer path, long size);

   public int utime(ByteBuffer path, int atime, int mtime);

   public int statfs(FuseStatfsSetter statfsSetter);

   public int open(ByteBuffer path, int flags, FuseOpenSetter openSetter);

   public int read(ByteBuffer path, Object fh, ByteBuffer buf, long offset);

   public int write(ByteBuffer path, Object fh, boolean isWritepage, ByteBuffer buf, long offset);

   public int flush(ByteBuffer path, Object fh);

   public int release(ByteBuffer path, Object fh, int flags);

   public int fsync(ByteBuffer path, Object fh, boolean isDatasync);

   //
   // extended attributes support contributed by Steven Pearson <steven_pearson@final-step.com>
   // and then modified by Peter Levart <peter@select-tech.si> to fit the new errno returning scheme

   public int setxattr(ByteBuffer path, ByteBuffer name, ByteBuffer value, int flags);

   public int getxattrsize(ByteBuffer path, ByteBuffer name, FuseSizeSetter sizeSetter);

   public int getxattr(ByteBuffer path, ByteBuffer name, ByteBuffer value);

   public int listxattrsize(ByteBuffer path, FuseSizeSetter sizeSetter);

   public int listxattr(ByteBuffer path, ByteBuffer list);

   public int removexattr(ByteBuffer path, ByteBuffer name);
}
