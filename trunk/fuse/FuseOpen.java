/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */
package fuse;

import fuse.util.Struct;

/**
 * An implementation of <code>FuseOpenSetter</code> interface that is passed as an argument to <code>fuse.Filesystem3.open()</code>
 * callback method to return filehandle and open options from it.
 */
public class FuseOpen extends Struct implements FuseOpenSetter
{
   public Object fh;
   public boolean directIO;
   public boolean keepCache;

   /**
    * Callback for filehandle API
    * <p/>
    * @param fh the filehandle to return from <code>open()<code> method.
    */
   public void setFh(Object fh)
   {
      this.fh = fh;
   }

   /**
    * Sets/gets the direct_io FUSE option for this opened file
    */
   public boolean isDirectIO()
   {
      return directIO;
   }

   public void setDirectIO(boolean directIO)
   {
      this.directIO = directIO;
   }

   /**
    * Sets/gets keep_cache FUSE option for this opened file
    */
   public boolean isKeepCache()
   {
      return keepCache;
   }

   public void setKeepCache(boolean keepCache)
   {
      this.keepCache = keepCache;
   }


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed)? ", " : " ");

      buff.append("fh=").append(fh)
          .append(", directIO=").append(directIO)
          .append(", keepCache=").append(keepCache);

      return true;
   }
}
