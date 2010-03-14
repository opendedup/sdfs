/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.compat.FuseStat;


public abstract class Node
{
   private String name;
   private FuseStat stat;
   private DirectoryNode parent;


   public Node(String name)
   {
      this.name = name;
      stat = createStat();
   }

   // to be implemented by subclasses

   protected abstract FuseStat createStat();

   // public API

   public synchronized String getName()
   {
      return name;
   }

   public synchronized void setName(String name)
   {
      this.name = name;
   }

   public synchronized FuseStat getStat()
   {
      return stat;
   }

   public synchronized void setStat(FuseStat stat)
   {
      this.stat = stat;
   }

   public synchronized DirectoryNode getParent()
   {
      return parent;
   }

   // just for fuse.staticfs internal usage

   synchronized void setParent(DirectoryNode parent)
   {
      this.parent = parent;
   }
}
