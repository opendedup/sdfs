/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;


public class ReadOnlySymlinkNode extends SymlinkNode
{
   private String target;

   public ReadOnlySymlinkNode(String name, String target)
   {
      super(name);
      
      this.target = target;
   }

   public synchronized String getTarget()
   {
      return target;
   }

   public synchronized void setTarget(String target)
   {
      this.target = target;
   }
}
