/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;
import java.util.*;


@SuppressWarnings({"OctalInteger"})
public class FakeFilesystem implements Filesystem3, XattrSupport
{
   private static final Log log = LogFactory.getLog(FakeFilesystem.class);

   private static final int BLOCK_SIZE = 512;
   private static final int NAME_LENGTH = 1024;

   private static class N
   {
      static int nfiles = 0;

      String name;
      int mode;
      Map<String,byte[]> xattrs = new HashMap<String,byte[]>();

      N(String name, int mode, String ... xattrs)
      {
         this.name = name;
         this.mode = mode;

         for (int i = 0; i < xattrs.length - 1; i += 2)
            this.xattrs.put(xattrs[i], xattrs[i + 1].getBytes());

         nfiles++;
      }

      public String toString()
      {
         String cn = getClass().getName();
         return cn.substring(cn.indexOf("$")) + "[ name=" + name + ", mode=" + Integer.toOctalString(mode) + "(OCT) ]";
      }
   }

   private static class D extends N
   {
      Map<String,N> files = new LinkedHashMap<String,N>();

      D(String name, int mode, String ... xattrs)
      {
         super(name, mode, xattrs);
      }

      void add(N n)
      {
         files.put(n.name, n);
      }

      public String toString()
      {
         return super.toString() + " with " + files.size() + " files";
      }
   }

   private static class F extends N
   {
      byte[] content;

      F(String name, int mode, String content, String ... xattrs)
      {
         super(name, mode, xattrs);

         this.content = content.getBytes();
      }
   }

   private static class L extends N
   {
      String link;

      L(String name, int mode, String link, String ... xattrs)
      {
         super(name, mode, xattrs);

         this.link = link;
      }
   }

   private static class FH
   {
      N n;

      FH(N n)
      {
         this.n = n;
         log.debug("  " + this + " created");
      }

      void release()
      {
         log.debug("  " + this + " released");
      }

      protected void finalize()
      {
         log.debug("  " + this + " finalized");
      }

      public String toString()
      {
         return "FH[" + n + ", hashCode=" + hashCode() + "]";
      }
   }


   // a root directory
   private D root;


   // lookup node
   private N lookup(String path)
   {
      if (path.equals("/"))
         return root;

      File f = new File(path);
      N parent = lookup(f.getParent());
      N node = (parent instanceof D)? ((D) parent).files.get(f.getName()) : null;

      if (log.isDebugEnabled())
         log.debug("  lookup(\"" + path + "\") returning: " + node);

      return node;
   }


   public FakeFilesystem()
   {
      root = new D("", 0755, "description", "ROOT directory");

      root.add(new F("README", 0644, "Hou have read me\n", "mimetype", "text/plain", "description", "a README file"));
      root.add(new F("execute_me.sh", 0755, "#!/bin/sh\n\necho \"You executed me\"\n", "mimetype", "text/plain", "description", "a BASH script"));

      D subdir = new D("subdir", 0755, "description", "a subdirectory");
      root.add(subdir);
      subdir.add(new L("README.link", 0666, "../README", "description", "a symbolic link"));
      subdir.add(new L("execute_me.link.sh", 0666, "../execute_me.sh", "description", "another symbolic link"));

      log.info("created");
   }


   public int chmod(String path, int mode) throws FuseException
   {
      N n = lookup(path);

      if (n != null)
      {
         n.mode = (n.mode & FuseStatConstants.TYPE_MASK) | (mode & FuseStatConstants.MODE_MASK);
         return 0;
      }

      return Errno.ENOENT;
   }

   public int chown(String path, int uid, int gid) throws FuseException
   {
      return 0;
   }

   public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException
   {
      N n = lookup(path);

      int time = (int) (System.currentTimeMillis() / 1000L);

      if (n instanceof D)
      {
         D d = (D) n;
         getattrSetter.set(
            d.hashCode(),
            FuseFtypeConstants.TYPE_DIR | d.mode,
            1,
            0, 0,
            0,
            d.files.size() * NAME_LENGTH,
            (d.files.size() * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE,
            time, time, time
         );

         return 0;
      }
      else if (n instanceof F)
      {
         F f = (F) n;
         getattrSetter.set(
            f.hashCode(),
            FuseFtypeConstants.TYPE_FILE | f.mode,
            1,
            0, 0,
            0,
            f.content.length,
            (f.content.length + BLOCK_SIZE - 1) / BLOCK_SIZE,
            time, time, time
         );

         return 0;
      }
      else if (n instanceof L)
      {
         L l = (L) n;
         getattrSetter.set(
            l.hashCode(),
            FuseFtypeConstants.TYPE_SYMLINK | l.mode,
            1,
            0, 0,
            0,
            l.link.length(),
            (l.link.length() + BLOCK_SIZE - 1) / BLOCK_SIZE,
            time, time, time
         );

         return 0;
      }

      return Errno.ENOENT;
   }

   public int getdir(String path, FuseDirFiller filler) throws FuseException
   {
      N n = lookup(path);

      if (n instanceof D)
      {
         for (N child : ((D) n).files.values())
         {
            int ftype = (child instanceof D)
                        ? FuseFtypeConstants.TYPE_DIR
                        : ((child instanceof F)
                           ? FuseFtypeConstants.TYPE_FILE
                           : ((child instanceof L)
                           ? FuseFtypeConstants.TYPE_SYMLINK
                           : 0));
            if (ftype > 0)
               filler.add(
                  child.name,
                  child.hashCode(),
                  ftype | child.mode
               );
         }

         return 0;
      }

      return Errno.ENOTDIR;
   }

   public int link(String from, String to) throws FuseException
   {
      return Errno.EROFS;
   }

   public int mkdir(String path, int mode) throws FuseException
   {
      return Errno.EROFS;
   }

   public int mknod(String path, int mode, int rdev) throws FuseException
   {
      return Errno.EROFS;
   }

   public int rename(String from, String to) throws FuseException
   {
      return Errno.EROFS;
   }

   public int rmdir(String path) throws FuseException
   {
      return Errno.EROFS;
   }

   public int statfs(FuseStatfsSetter statfsSetter) throws FuseException
   {
      statfsSetter.set(
         BLOCK_SIZE,
         1000,
         200,
         180,
         N.nfiles,
         0,
         NAME_LENGTH
      );

      return 0;
   }

   public int symlink(String from, String to) throws FuseException
   {
      return Errno.EROFS;
   }

   public int truncate(String path, long size) throws FuseException
   {
      return Errno.EROFS;
   }

   public int unlink(String path) throws FuseException
   {
      return Errno.EROFS;
   }

   public int utime(String path, int atime, int mtime) throws FuseException
   {
      return 0;
   }

   public int readlink(String path, CharBuffer link) throws FuseException
   {
      N n = lookup(path);

      if (n instanceof L)
      {
         link.append(((L) n).link);
         return 0;
      }

      return Errno.ENOENT;
   }

   // if open returns a filehandle by calling FuseOpenSetter.setFh() method, it will be passed to every method that supports 'fh' argument
   public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException
   {
      N n = lookup(path);

      if (n != null)
      {
         openSetter.setFh(new FH(n));
         return 0;
      }

      return Errno.ENOENT;
   }

   // fh is filehandle passed from open,
   // isWritepage indicates that write was caused by a writepage
   public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException
   {
      return Errno.EROFS;
   }

   // fh is filehandle passed from open
   public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException
   {
      if (fh instanceof FH)
      {
         F f = (F) ((FH) fh).n;
         buf.put(f.content, (int) offset, Math.min(buf.remaining(), f.content.length - (int)offset));

         return 0;
      }

      return Errno.EBADF;
   }

   // new operation (called on every filehandle close), fh is filehandle passed from open
   public int flush(String path, Object fh) throws FuseException
   {
      if (fh instanceof FH)
         return 0;

      return Errno.EBADF;
   }

   // new operation (Synchronize file contents), fh is filehandle passed from open,
   // isDatasync indicates that only the user data should be flushed, not the meta data
   public int fsync(String path, Object fh, boolean isDatasync) throws FuseException
   {
      if (fh instanceof FH)
         return 0;

      return Errno.EBADF;
   }

   // (called when last filehandle is closed), fh is filehandle passed from open
   public int release(String path, Object fh, int flags) throws FuseException
   {
      if (fh instanceof FH)
      {
         ((FH) fh).release();
         System.runFinalization();
         return 0;
      }

      return Errno.EBADF;
   }

   //
   // XattrSupport implementation

   /**
    * This method will be called to get the value of the extended attribute
    *
    * @param path the path to file or directory containing extended attribute
    * @param name the name of the extended attribute
    * @param dst  a ByteBuffer that should be filled with the value of the extended attribute
    * @return 0 if Ok or errno when error
    * @throws fuse.FuseException an alternative to returning errno is to throw this exception with errno initialized
    * @throws java.nio.BufferOverflowException
    *                            should be thrown to indicate that the given <code>dst</code> ByteBuffer
    *                            is not large enough to hold the attribute's value. After that <code>getxattr()</code> method will
    *                            be called again with a larger buffer.
    */
   public int getxattr(String path, String name, ByteBuffer dst) throws FuseException, BufferOverflowException
   {
      N n = lookup(path);

      if (n == null)
         return Errno.ENOENT;

      byte[] value = n.xattrs.get(name);

      if (value == null)
         return Errno.ENOATTR;

      dst.put(value);

      return 0;
   }

   /**
    * This method can be called to query for the size of the extended attribute
    *
    * @param path       the path to file or directory containing extended attribute
    * @param name       the name of the extended attribute
    * @param sizeSetter a callback interface that should be used to set the attribute's size
    * @return 0 if Ok or errno when error
    * @throws fuse.FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException
   {
      N n = lookup(path);

      if (n == null)
         return Errno.ENOENT;

      byte[] value = n.xattrs.get(name);

      if (value == null)
         return Errno.ENOATTR;

      sizeSetter.setSize(value.length);

      return 0;
   }

   /**
    * This method will be called to get the list of extended attribute names
    *
    * @param path   the path to file or directory containing extended attributes
    * @param lister a callback interface that should be used to list the attribute names
    * @return 0 if Ok or errno when error
    * @throws fuse.FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int listxattr(String path, XattrLister lister) throws FuseException
   {
      N n = lookup(path);

      if (n == null)
         return Errno.ENOENT;

      for (String xattrName : n.xattrs.keySet())
         lister.add(xattrName);

      return 0;
   }

   /**
    * This method will be called to remove the extended attribute
    *
    * @param path the path to file or directory containing extended attributes
    * @param name the name of the extended attribute
    * @return 0 if Ok or errno when error
    * @throws fuse.FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int removexattr(String path, String name) throws FuseException
   {
      return Errno.EROFS;
   }

   /**
    * This method will be called to set the value of an extended attribute
    *
    * @param path  the path to file or directory containing extended attributes
    * @param name  the name of the extended attribute
    * @param value the value of the extended attribute
    * @param flags parameter can be used to refine the semantics of the operation.<p>
    *              <code>XATTR_CREATE</code> specifies a pure create, which should fail with <code>Errno.EEXIST</code> if the named attribute exists already.<p>
    *              <code>XATTR_REPLACE</code> specifies a pure replace operation, which should fail with <code>Errno.ENOATTR</code> if the named attribute does not already exist.<p>
    *              By default (no flags), the  extended  attribute  will  be created if need be, or will simply replace the value if the attribute exists.
    * @return 0 if Ok or errno when error
    * @throws fuse.FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int setxattr(String path, String name, ByteBuffer value, int flags) throws FuseException
   {
      return Errno.EROFS;
   }


   //
   // Java entry point

   public static void main(String[] args)
   {
      log.info("entering");

      try
      {
         FuseMount.mount(args, new FakeFilesystem(), log);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         log.info("exiting");
      }
   }
}
