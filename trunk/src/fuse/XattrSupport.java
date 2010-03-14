package fuse;

import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;

/**
 * if fuse.Filesystem[123] implementation also implements this interface, then it supports extended attributes
 */
public interface XattrSupport
{
   // bits passed in 'flags' parameter to setxattr() method

   public static final int XATTR_CREATE = 0x1;        /* set value, fail if attr already exists */
   public static final int XATTR_REPLACE = 0x2;       /* set value, fail if attr does not exist */

   /**
    * This method can be called to query for the size of the extended attribute
    *
    * @param path the path to file or directory containing extended attribute
    * @param name the name of the extended attribute
    * @param sizeSetter a callback interface that should be used to set the attribute's size
    * @return 0 if Ok or errno when error
    * @throws FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException;

   /**
    * This method will be called to get the value of the extended attribute
    *
    * @param path the path to file or directory containing extended attribute
    * @param name the name of the extended attribute
    * @param dst a ByteBuffer that should be filled with the value of the extended attribute
    * @return 0 if Ok or errno when error
    * @throws FuseException an alternative to returning errno is to throw this exception with errno initialized
    * @throws BufferOverflowException should be thrown to indicate that the given <code>dst</code> ByteBuffer
    *         is not large enough to hold the attribute's value. After that <code>getxattr()</code> method will
    *         be called again with a larger buffer.
    */
   public int getxattr(String path, String name, ByteBuffer dst) throws FuseException, BufferOverflowException;

   /**
    * This method will be called to get the list of extended attribute names
    *
    * @param path the path to file or directory containing extended attributes
    * @param lister a callback interface that should be used to list the attribute names
    * @return 0 if Ok or errno when error
    * @throws FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int listxattr(String path, XattrLister lister) throws FuseException;

   /**
    * This method will be called to set the value of an extended attribute
    *
    * @param path the path to file or directory containing extended attributes
    * @param name the name of the extended attribute
    * @param value the value of the extended attribute
    * @param flags parameter can be used to refine the semantics of the operation.<p>
    *        <code>XATTR_CREATE</code> specifies a pure create, which should fail with <code>Errno.EEXIST</code> if the named attribute exists already.<p>
    *        <code>XATTR_REPLACE</code> specifies a pure replace operation, which should fail with <code>Errno.ENOATTR</code> if the named attribute does not already exist.<p>
    *        By default (no flags), the  extended  attribute  will  be created if need be, or will simply replace the value if the attribute exists.
    * @return 0 if Ok or errno when error
    * @throws FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int setxattr(String path, String name, ByteBuffer value, int flags) throws FuseException;

   /**
    * This method will be called to remove the extended attribute
    *
    * @param path the path to file or directory containing extended attributes
    * @param name the name of the extended attribute
    * @return 0 if Ok or errno when error
    * @throws FuseException an alternative to returning errno is to throw this exception with errno initialized
    */
   public int removexattr(String path, String name) throws FuseException;
}
