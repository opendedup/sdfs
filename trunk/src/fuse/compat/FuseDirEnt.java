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

/**
 * This is a String level API directory entry used in fuse.compat.Filesystem1 and fuse.compat.Filesystem2 compatibility APIs
 */
public class FuseDirEnt extends FuseFtype
{
   public String name;

   // CHANGE-22: inode added
   public int inode;


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(isPrefixed? ", " : " ").append("name='").append(name).append("'").append("inode='").append(inode).append("'");

      return super.appendAttributes(buff, true);
   }
}
