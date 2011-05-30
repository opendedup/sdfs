/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;


public class FuseException extends Exception implements Errno
{
   private static final long serialVersionUID = 0;
   
   private int errno;

   public FuseException()
   {
   }

   public FuseException(Throwable cause)
   {
      super(cause);
   }

   public FuseException(String message)
   {
      super(message);
   }

   public FuseException(String message, Throwable cause)
   {
      super(message, cause);
   }

   public FuseException initErrno(int errno)
   {
      this.errno = errno;

      return this;
   }

   public int getErrno()
   {
      return errno;
   }
}
