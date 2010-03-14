/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;


/**
 * This is a byte level API directory entry
 */

public class FuseFSDirEnt extends FuseFtype
{
   public byte[] name;

   public long inode;


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(isPrefixed? ", " : " ").append("name='").append(name).append("'").append("inode='").append(inode).append("'");

      return super.appendAttributes(buff, true);
   }
}
