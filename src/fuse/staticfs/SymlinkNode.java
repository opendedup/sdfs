/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.FuseFtype;
import fuse.compat.FuseStat;


public abstract class SymlinkNode extends Node
{
   public SymlinkNode(String name)
   {
      super(name);
   }

   //
   // create initial FuseStat structure (called from Node's constructor)

   protected FuseStat createStat()
   {
      FuseStat stat = new FuseStat();

      stat.mode = FuseFtype.TYPE_SYMLINK | 0777;
      stat.uid = stat.gid = 0;
      stat.ctime = stat.mtime = stat.atime = (int)(System.currentTimeMillis() / 1000L);
      stat.size = 0;
      stat.blocks = 0;

      return stat;
   }

   //
   // public API

   public abstract String getTarget();
}
