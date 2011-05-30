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
 * Filesystem constants common to all filesystem interfaces
 */
public interface FilesystemConstants
{
   public static final int O_RDONLY = 00;
   public static final int O_WRONLY = 01;
   public static final int O_RDWR = 02;   
}
