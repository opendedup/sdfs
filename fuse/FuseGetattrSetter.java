package fuse;

/**
 * User: peter
 * Date: Nov 3, 2005
 * Time: 4:16:10 PM
 */
public interface FuseGetattrSetter
{
   public void set(long inode, int mode, int nlink, int uid, int gid, int rdev, long size, long blocks, int atime, int mtime, int ctime);
}
