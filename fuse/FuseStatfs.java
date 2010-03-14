/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse;

import fuse.util.Struct;


public class FuseStatfs extends Struct implements FuseStatfsSetter
{
   public int blockSize;
   public int blocks;
   public int blocksFree;
   public int blocksAvail = -1; // by default (if not overwriten) it is computed as: blocksFree * 20 / 19
   public int files;
   public int filesFree;
   public int namelen;


   /**
    * FuseStatfsSetter implementation
    */
   public void set(int blockSize, int blocks, int blocksFree, int blocksAvail, int files, int filesFree, int namelen)
   {
      this.blockSize = blockSize;
      this.blocks = blocks;
      this.blocksFree = blocksFree;
      this.blocksAvail = (blocksAvail >= 0)? blocksAvail : (int)((long)blocksFree * 20L / 19L);
      this.files = files;
      this.filesFree = filesFree;
      this.namelen = namelen;
   }


   protected boolean appendAttributes(StringBuilder buff, boolean isPrefixed)
   {
      buff.append(super.appendAttributes(buff, isPrefixed)? ", " : " ");

      buff.append("blockSize=").append(blockSize)
         .append(", blocks=").append(blocks)
         .append(", blocksFree=").append(blocksFree)
         .append(", blocksAvail=").append(blocksAvail)
         .append(", files=").append(files)
         .append(", filesFree=").append(filesFree)
         .append(", namelen=").append(namelen);

      return true;
   }
}
