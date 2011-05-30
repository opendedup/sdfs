package fuse;

/**
 * Created by IntelliJ IDEA.
 * User: peter
 * Date: Jan 1, 2006
 * Time: 6:21:06 PM
 * To change this template use File | Settings | File Templates.
 */
public interface FuseFillDir
{
   /**
    * Method to add an entry in a readdir() operation
    *
    * @param name the name of the entry
    * @param inode the inode number of the entry (optional)
    * @param mode the entry type bits (from fuse.FuseFtypeConstants)
    * @param nextOffset the offset of next entry (in streaming mode) or zero (in buffering mode)
    * @return true if successfull (allways if buffering) or false if buffer full (in streaming mode)
    */
   public boolean fill(String name, long inode, int mode, long nextOffset);
}
