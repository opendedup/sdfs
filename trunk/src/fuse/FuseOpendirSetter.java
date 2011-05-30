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
 * A callback interface used in <code>fuse.Filesystem3.opendir()</code> method
 */
public interface FuseOpendirSetter
{
   /**
    * Callback for filehandle API
    * <p/>
    * @param fh the filehandle to return from <code>opendir()</code> method.
    */
   public void setFh(Object fh);
}
