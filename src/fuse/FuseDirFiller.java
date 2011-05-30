package fuse;

/**
 * User: peter
 * Date: Nov 3, 2005
 * Time: 2:54:06 PM
 */
public interface FuseDirFiller
{
   public void add(String name, long inode, int mode);
}
