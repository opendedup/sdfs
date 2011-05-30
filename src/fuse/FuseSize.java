package fuse;

import fuse.util.Struct;

/**
 * User: peter
 * Date: Nov 16, 2005
 * Time: 10:04:26 AM
 */
public class FuseSize extends Struct implements FuseSizeSetter
{
   public int size;

   //
   // FuseSizeSetter implementation

   public void setSize(int size)
   {
      this.size = size;
   }


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed) ? ", " : " ");

      buff.append("size=").append(size);

      return true;
   }
}
