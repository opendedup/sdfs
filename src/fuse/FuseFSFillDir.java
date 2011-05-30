package fuse;

import fuse.util.Struct;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by IntelliJ IDEA.
 * User: peter
 * Date: Jan 1, 2006
 * Time: 6:29:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class FuseFSFillDir extends Struct implements FuseFillDir
{
   private Charset cs;   // charset to use for encoding file names
   private long buf;     // native buffer pointer stored in 64 bit long
   private long fillDir; // native pointer to fuse_fill_dir_t function stored in 64 bit long


   FuseFSFillDir(Charset cs, long buf, long fillDir)
   {
      this.cs = cs;
      this.buf = buf;
      this.fillDir = fillDir;
   }


   /**
    * Method to add an entry in a readdir() operation
    *
    * @param name       the name of the entry
    * @param inode      the inode number of the entry (optional)
    * @param mode       the entry type bits (from fuse.FuseFtypeConstants)
    * @param nextOffset the offset of next entry (in streaming mode) or zero (in buffering mode)
    * @return true if successfull (allways if buffering) or false if buffer full (in streaming mode)
    */
   public boolean fill(String name, long inode, int mode, long nextOffset)
   {
      // encode into native ByteBuffer terminated with (byte)0
      ByteBuffer bb = cs.encode(name);
      ByteBuffer nbb = ByteBuffer.allocateDirect(bb.remaining() + 1);
      nbb.put(bb);
      nbb.put((byte) 0);
      nbb.flip();

      return fill(nbb, inode, mode, nextOffset, buf, fillDir);
   }

   /**
    * Native method that uses fillDir value as a pointer to fuse_fill_dir_t function and
    * calls that function with converted parameters...
    *
    * @param name the name of the entry encoded in given charset as a direct ByteBuffer
    * @param inode the inode number of the entry (optional)
    * @param mode the entry type bits (from fuse.FuseFtypeConstants)
    * @param nextOffset the offset of next entry (in streaming mode) or zero (in buffering mode)
    * @param buf native buffer pointer stored in 64 bit long
    * @param fillDir native pointer to fuse_fill_dir_t function stored in 64 bit long
    * @return true if successfull (allways if buffering) or false if buffer full (in streaming mode)
    */
   private native boolean fill(ByteBuffer name, long inode, int mode, long nextOffset, long buf, long fillDir);


   //
   // Struct subclass

   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed)? ", " : " ");

      buff.append("cs=").append(cs)
          .append(", buf=").append(buf)
          .append(", fillDir=").append(fillDir);

      return true;
   }
}
