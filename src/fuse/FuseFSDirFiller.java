package fuse;

import java.util.List;
import java.util.ArrayList;
import java.nio.charset.Charset;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;

/**
 * User: peter
 * Date: Nov 4, 2005
 * Time: 1:36:36 PM
 */
public class FuseFSDirFiller extends ArrayList<FuseFSDirEnt> implements FuseDirFiller
{
   private Charset cs;

   public void setCharset(Charset cs)
   {
      this.cs = cs;
   }

   //
   // FuseDirFiller implementation

   public void add(String name, long inode, int mode)
   {
      FuseFSDirEnt dirEntry = new FuseFSDirEnt();

      ByteBuffer nameBuf = cs.encode(CharBuffer.wrap(name));
      dirEntry.name = new byte[nameBuf.remaining()];
      nameBuf.get(dirEntry.name);

      dirEntry.inode = inode;

      dirEntry.mode = mode;

      add(dirEntry);
   }

   //
   // for debugging

   /**
    * Returns a string representation of this collection.  The string
    * representation consists of a list of the collection's elements in the
    * order they are returned by its iterator, enclosed in square brackets
    * (<tt>"[]"</tt>).  Adjacent elements are separated by the characters
    * <tt>", "</tt> (comma and space).  Elements are converted to strings as
    * by <tt>String.valueOf(Object)</tt>.<p>
    * <p/>
    * This implementation creates an empty string buffer, appends a left
    * square bracket, and iterates over the collection appending the string
    * representation of each element in turn.  After appending each element
    * except the last, the string <tt>", "</tt> is appended.  Finally a right
    * bracket is appended.  A string is obtained from the string buffer, and
    * returned.
    *
    * @return a string representation of this collection.
    */
   public String toString()
   {
      StringBuilder sb = new StringBuilder();

      sb.append("[");
      boolean first = true;

      for (FuseFSDirEnt dirEnt : this)
      {
         if (first)
            first = false;
         else
            sb.append(", ");

         sb.append('"').append(cs.decode(ByteBuffer.wrap(dirEnt.name))).append('"');
      }

      sb.append("]");

      return sb.toString();
   }
}
