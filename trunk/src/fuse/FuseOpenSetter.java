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
 * A callback interface used in <code>fuse.Filesystem3.open()</code> method
 */
public interface FuseOpenSetter
{
   /**
    * Callback for filehandle API
    * <p/>
    * @param fh the filehandle to return from <code>open()<code> method.
    */
   public void setFh(Object fh);

   /**
    * Sets/gets the direct_io FUSE option for this opened file
    */
   public boolean isDirectIO();

   public void setDirectIO(boolean directIO);

   /**
    * Sets/gets keep_cache FUSE option for this opened file
    */
   public boolean isKeepCache();

   public void setKeepCache(boolean keepCache);
}
