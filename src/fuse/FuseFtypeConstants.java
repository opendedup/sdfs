package fuse;

/**
 * User: peter
 * Date: Nov 11, 2005
 * Time: 10:14:14 AM
 */
@SuppressWarnings({"OctalInteger"})
public interface FuseFtypeConstants
{
   // file type 'mode' bits

   public static final int TYPE_MASK     = 0170000;   // bitmask for the file type bitfields
   
   public static final int TYPE_SOCKET   = 0140000;   // socket
   public static final int TYPE_SYMLINK  = 0120000;   // symbolic link
   public static final int TYPE_FILE     = 0100000;   // regular file
   public static final int TYPE_BLOCKDEV = 0060000;   // block device
   public static final int TYPE_DIR      = 0040000;   // directory
   public static final int TYPE_CHARDEV  = 0020000;   // character device
   public static final int TYPE_FIFO     = 0010000;   // fifo
}
