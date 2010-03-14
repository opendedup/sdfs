package fuse;

import fuse.util.Struct;

/**
 * Java counterpart of struct fuse_context FUSE C API.
 * Every instance is filled-in with current Thread's active FUSE context which is
 * only relevant for the duration of a filesystem operation
 */
public class FuseContext extends Struct
{
   public int uid;
   public int gid;
   public int pid;

   private FuseContext()
   {
   }


   public static FuseContext get()
   {
      FuseContext fuseContext = new FuseContext();
      fuseContext.fillInFuseContext();
      return fuseContext;
   }


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed)? ", " : " ");

      buff.append("uid=").append(uid)
          .append(", gid=").append(gid)
          .append(", pid=").append(pid);

      return true;
   }


   private native void fillInFuseContext();
}
