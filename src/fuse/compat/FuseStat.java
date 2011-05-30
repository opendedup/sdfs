/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.compat;

import fuse.FuseFtype;
import fuse.FuseFtypeConstants;


public class FuseStat extends FuseFtype implements FuseFtypeConstants
{
   public int nlink;
   public int uid;
   public int gid;
   public long size;
   public int atime;
   public int mtime;
   public int ctime;
   public int blocks;

   // inode support fix by Edwin Olson <eolson@mit.edu>
   public long inode;


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed)? ", " : " ");

      buff.append("nlink=").append(nlink)
          .append(", uid=").append(uid)
          .append(", gid=").append(gid)
          .append(", size=").append(size)
          .append(", atime=").append(atime)
          .append(", mtime=").append(mtime)
          .append(", ctime=").append(ctime)
          .append(", blocks=").append(blocks)
          .append(", inode=").append(inode);

      return true;
   }
}
