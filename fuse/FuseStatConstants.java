package fuse;

/**
 * User: peter
 * Date: Nov 11, 2005
 * Time: 10:15:48 AM
 */
@SuppressWarnings({"OctalInteger"})
public interface FuseStatConstants extends FuseFtypeConstants
{
   // additional mode bits

   public static final int MODE_MASK     = 0007777;   // mode bits mask
   
   public static final int SUID_BIT      = 0004000;   // set UID bit
   public static final int SGID_BIT      = 0002000;   // set GID bit
   public static final int STICKY_BIT    = 0001000;   // sticky bit

   public static final int OWNER_MASK    = 0000700;   // mask for file owner permissions
   public static final int OWNER_READ    = 0000400;   // owner has read permission
   public static final int OWNER_WRITE   = 0000200;   // owner has write permission
   public static final int OWNER_EXECUTE = 0000100;   // owner has execute permission

   public static final int GROUP_MASK    = 0000070;   // mask for group permissions
   public static final int GROUP_READ    = 0000040;   // group has read permission
   public static final int GROUP_WRITE   = 0000020;   // group has write permission
   public static final int GROUP_EXECUTE = 0000010;   // group has execute permission

   public static final int OTHER_MASK    = 0000007;   // mask for permissions for others
   public static final int OTHER_READ    = 0000004;   // others have read permission
   public static final int OTHER_WRITE   = 0000002;   // others have write permisson
   public static final int OTHER_EXECUTE = 0000001;   // others have execute permission
}
