/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.compat.Filesystem1;
import fuse.compat.FuseStat;


public class MountpointNode extends Node
{
   private Filesystem1 filesystem;

   public MountpointNode(String name, Filesystem1 filesystem)
   {
      super(name);

      this.filesystem = filesystem;
   }

   protected FuseStat createStat()
   {
      return null;
   }

   public Filesystem1 getFilesystem()
   {
      return filesystem;
   }
}
