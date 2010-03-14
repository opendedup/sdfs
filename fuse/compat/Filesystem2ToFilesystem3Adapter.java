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
import java.nio.CharBuffer;

/**
 * This is an adapter that adapts the semi-old filehandle enabled API (fuse.compat.Filesystem2) to
 * the new errno-as-return-value-or-exception API (fuse.Filesystem3)
 */
@SuppressWarnings({"UnnecessaryUnboxing", "UnnecessaryBoxing"})
public class Filesystem2ToFilesystem3Adapter implements Filesystem3
{
   private Filesystem2 fs2;

   public Filesystem2ToFilesystem3Adapter(Filesystem2 fs2)
   {
      this.fs2 = fs2;
   }

   public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException
   {
      FuseStat stat = fs2.getattr(path);

      getattrSetter.set(
         stat.inode,
         stat.mode,
         stat.nlink,
         stat.uid,
         stat.gid,
         0,
         stat.size,
         stat.blocks,
         stat.atime,
         stat.mtime,
         stat.ctime
      );

      return 0;
   }

   public int chmod(String path, int mode) throws FuseException
   {
      fs2.chmod(path, mode);

      return 0;
   }

   public int chown(String path, int uid, int gid) throws FuseException
   {
      fs2.chown(path, uid, gid);

      return 0;
   }

   // called on every filehandle close, fh is filehandle passed from open
   public int flush(String path, Object fh) throws FuseException
   {
      fs2.flush(path, (fh == null)? 0L : ((Long) fh).longValue());

      return 0;
   }

   // Synchronize file contents, fh is filehandle passed from open,
   // isDatasync indicates that only the user data should be flushed, not the meta data
   public int fsync(String path, Object fh, boolean isDatasync) throws FuseException
   {
      fs2.fsync(path, (fh == null)? 0L : ((Long) fh).longValue(), isDatasync);

      return 0;
   }

   public int getdir(String path, FuseDirFiller filler) throws FuseException
   {
      for (FuseDirEnt entry : fs2.getdir(path))
         filler.add(entry.name, entry.inode, entry.mode);

      return 0;
   }

   public int link(String from, String to) throws FuseException
   {
      fs2.link(from, to);

      return 0;
   }

   public int mkdir(String path, int mode) throws FuseException
   {
      fs2.mkdir(path, mode);

      return 0;
   }

   public int mknod(String path, int mode, int rdev) throws FuseException
   {
      fs2.mknod(path, mode, rdev);

      return 0;
   }

   // if open returns a filehandle by calling FuseOpenSetter.setFh() method, it will be passed to every method that supports 'fh' argument
   public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException
   {
      openSetter.setFh(new Long(fs2.open(path, flags)));

      return 0;
   }

   // fh is filehandle passed from open
   public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException
   {
      fs2.read(path, (fh == null)? 0L : ((Long) fh).longValue(), buf, offset);

      return 0;
   }

   public int readlink(String path, CharBuffer link) throws FuseException
   {
      link.put(fs2.readlink(path));

      return 0;
   }

   // called when last filehandle is closed, fh is filehandle passed from open
   public int release(String path, Object fh, int flags) throws FuseException
   {
      fs2.release(path,  (fh == null)? 0L : ((Long) fh).longValue(), flags);

      return 0;
   }

   public int rename(String from, String to) throws FuseException
   {
      fs2.rename(from, to);

      return 0;
   }

   public int rmdir(String path) throws FuseException
   {
      fs2.rmdir(path);

      return 0;
   }

   public int statfs(FuseStatfsSetter statfsSetter) throws FuseException
   {
      FuseStatfs statfs = fs2.statfs();

      statfsSetter.set(
         statfs.blockSize,
         statfs.blocks,
         statfs.blocksFree,
         statfs.blocksAvail,
         statfs.files,
         statfs.filesFree,
         statfs.namelen
      );

      return 0;
   }

   public int symlink(String from, String to) throws FuseException
   {
      fs2.symlink(from, to);

      return 0;
   }

   public int truncate(String path, long size) throws FuseException
   {
      fs2.truncate(path, size);

      return 0;
   }

   public int unlink(String path) throws FuseException
   {
      fs2.unlink(path);

      return 0;
   }

   public int utime(String path, int atime, int mtime) throws FuseException
   {
      fs2.utime(path, atime, mtime);

      return 0;
   }

   // fh is filehandle passed from open,
   // isWritepage indicates that write was caused by a writepage
   public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException
   {
      fs2.write(path,  (fh == null)? 0L : ((Long) fh).longValue(), isWritepage, buf, offset);

      return 0;
   }
}
