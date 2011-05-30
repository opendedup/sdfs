/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;

import fuse.FuseFtype;
import fuse.FuseGetattrSetter;


public class FuseGetattr extends FuseFtype implements FuseGetattrSetter, FuseStatConstants
{
   public long inode;
   // public int mode; in superclass
   public int nlink;
   public int uid;
   public int gid;
   public int rdev;
   public long size;
   public long blocks;
   public int atime;
   public int mtime;
   public int ctime;


   //
   // FuseGetattrSetter implementation

   public void set(long inode, int mode, int nlink, int uid, int gid, int rdev, long size, long blocks, int atime, int mtime, int ctime)
   {
      this.inode = inode;
      this.mode = mode;
      this.nlink = nlink;
      this.uid = uid;
      this.gid = gid;
      this.rdev = rdev;
      this.size = size;
      this.blocks = blocks;
      this.atime = atime;
      this.mtime = mtime;
      this.ctime = ctime;
   }


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed)? ", " : " ");

      buff
         .append("inode=").append(inode)
         .append(", nlink=").append(nlink)
         .append(", uid=").append(uid)
         .append(", gid=").append(gid)
         .append(", rdev=").append(rdev)
         .append(", size=").append(size)
         .append(", blocks=").append(blocks)
         .append(", atime=").append(atime)
         .append(", mtime=").append(mtime)
         .append(", ctime=").append(ctime);

      return true;
   }
}
