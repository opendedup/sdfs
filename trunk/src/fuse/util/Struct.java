/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.util;


public abstract class Struct implements Cloneable
{
   public Object clone()
   {
      try
      {
         return super.clone();
      }
      catch (CloneNotSupportedException e)
      {
         // will not happen
         throw new RuntimeException(e);
      }
   }

   public String toString()
   {
      StringBuilder sb = new StringBuilder(getClass().getName());

      return sb
         .append("[ ")
         .append(appendAttributes(sb, false)? ", " : "")
         .append("hashCode=").append(hashCode())
         .append(" ]")
         .toString();
   }

   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      return isPrefixed;
   }
}
