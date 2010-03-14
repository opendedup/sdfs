package java2c;

import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: peter
 * Date: May 18, 2005
 * Time: 11:27:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class DumpJVMLdPath
{
   public static void main(String[] args) throws IOException
   {
      String[] libDirs = System.getProperty("java.library.path", "").split(":");

      StringBuffer sb = new StringBuffer("LDPATH :=");
      for (int i = 0; i < libDirs.length; i++)
         sb.append(" -L").append(libDirs[i]);

      if (args.length == 0)
         System.out.println(sb.toString());
      else
      {
         PrintStream out = new PrintStream(new FileOutputStream(args[0]));
         out.println(sb.toString());
         out.close();
      }
   }
}
