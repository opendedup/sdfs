/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.*;
import fuse.compat.Filesystem1;
import fuse.compat.FuseDirEnt;
import fuse.compat.FuseStat;

import java.nio.ByteBuffer;


public class StaticFilesystem implements Filesystem1
{
   DirectoryNode rootNode;
   FuseStatfs statfs;


   public StaticFilesystem(DirectoryNode rootNode)
   {
      this.rootNode = rootNode;

      statfs = new FuseStatfs();
      statfs.blocks = 0;
      statfs.blocksFree = 0;
      statfs.blockSize = 8192;
      statfs.files = 0;
      statfs.filesFree = 0;
      statfs.namelen = 2048;
   }

   public DirectoryNode getRootNode()
   {
      return rootNode;
   }

   //
   // Filesystem implementation

   public void chmod(String path, int mode) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().chmod(rr.path, mode);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void chown(String path, int uid, int gid) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().chown(rr.path, uid, gid);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public FuseStat getattr(String path) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         return ((MountpointNode) rr.node).getFilesystem().getattr(rr.path);
      }

      return rr.node.getStat();
   }

   public FuseDirEnt[] getdir(String path) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         return ((MountpointNode) rr.node).getFilesystem().getdir(rr.path);
      }

      if (!(rr.node instanceof DirectoryNode))
         throw new FuseException("Not a Directory").initErrno(FuseException.ENOTDIR);

      Node[] children = ((DirectoryNode) rr.node).getChildren();
      FuseDirEnt[] dirEntries = new FuseDirEnt[children.length];
      for (int i = 0; i < children.length; i++)
      {
         Node child = children[i];
         FuseStat stat = (child instanceof MountpointNode)
            ? ((MountpointNode) child).getFilesystem().getattr("/")
            : child.getStat();
         FuseDirEnt dirEntry = new FuseDirEnt();
         dirEntry.name = child.getName();
         dirEntry.mode = stat.mode;
         dirEntries[i] = dirEntry;
      }

      return dirEntries;
   }

   public void link(String from, String to) throws FuseException
   {
      ResolveResult fromRr = resolvePath(from);
      ResolveResult toRr = resolveParentPath(to);

      boolean fromIsMount = fromRr.node instanceof MountpointNode;
      boolean toIsMount = toRr.node instanceof MountpointNode;

      if (fromIsMount || toIsMount)
      {
         if (fromIsMount && toIsMount && fromRr.node == toRr.node)
         {
            ((MountpointNode) fromRr.node).getFilesystem().link(fromRr.path, toRr.path);
         }
         else
         {
            throw new FuseException("Cross Device Link not possible").initErrno(FuseException.EXDEV);
         }
      }
      else
      {
         throw new FuseException("Read Only").initErrno(FuseException.EROFS);
      }
   }

   public void mkdir(String path, int mode) throws FuseException
   {
      ResolveResult rr = resolveParentPath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().mkdir(rr.path, mode);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void mknod(String path, int mode, int rdev) throws FuseException
   {
      ResolveResult rr = resolveParentPath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().mknod(rr.path, mode, rdev);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void open(String path, int flags) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().open(rr.path, flags);
         return;
      }

      if (rr.node instanceof FileNode)
      {
         ((FileNode) rr.node).open(flags);
         return;
      }

      throw new FuseException("Not a File").initErrno(FuseException.EINVAL);
   }

   public void read(String path, ByteBuffer buf, long offset) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().read(rr.path, buf, offset);
         return;
      }

      if (rr.node instanceof FileNode)
      {
         ((FileNode) rr.node).read(buf, offset);
         return;
      }

      throw new FuseException("Not a File").initErrno(FuseException.EINVAL);
   }

   public String readlink(String path) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         return ((MountpointNode) rr.node).getFilesystem().readlink(rr.path);
      }

      if (rr.node instanceof SymlinkNode)
      {
         return ((SymlinkNode) rr.node).getTarget();
      }

      throw new FuseException("Not a Symbolic Link").initErrno(FuseException.EINVAL);
   }

   public void release(String path, int flags) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().release(rr.path, flags);
         return;
      }

      if (rr.node instanceof FileNode)
      {
         ((FileNode) rr.node).release(flags);
         return;
      }

      throw new FuseException("Not a File").initErrno(FuseException.EINVAL);
   }

   public void rename(String from, String to) throws FuseException
   {
      ResolveResult fromRr = resolvePath(from);
      ResolveResult toRr = resolveParentPath(to);

      boolean fromIsMount = fromRr.node instanceof MountpointNode;
      boolean toIsMount = toRr.node instanceof MountpointNode;

      if (fromIsMount || toIsMount)
      {
         if (fromIsMount && toIsMount && fromRr.node == toRr.node)
         {
            ((MountpointNode) fromRr.node).getFilesystem().rename(fromRr.path, toRr.path);
         }
         else
         {
            throw new FuseException("Cross Device Rename not possible").initErrno(FuseException.EXDEV);
         }
      }
      else
      {
         throw new FuseException("Read Only").initErrno(FuseException.EROFS);
      }
   }

   public void rmdir(String path) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().rmdir(rr.path);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public FuseStatfs statfs() throws FuseException
   {
      return statfs;
   }

   public void symlink(String from, String to) throws FuseException
   {
      ResolveResult rr = resolveParentPath(to);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().symlink(from, rr.path);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void truncate(String path, long size) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().truncate(rr.path, size);
         return;
      }

      if (rr.node instanceof FileNode)
      {
         ((FileNode) rr.node).truncate(size);
         return;
      }

      throw new FuseException("Not a File").initErrno(FuseException.EINVAL);
   }

   public void unlink(String path) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().unlink(rr.path);
         return;
      }

      throw new FuseException("Read Only").initErrno(FuseException.EROFS);
   }

   public void utime(String path, int atime, int mtime) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().utime(rr.path, atime, mtime);
         return;
      }

      if (rr.node instanceof FileNode)
      {
         ((FileNode) rr.node).utime(atime, mtime);
         return;
      }

      throw new FuseException("Not a File").initErrno(FuseException.EINVAL);
   }

   public void write(String path, ByteBuffer buf, long offset) throws FuseException
   {
      ResolveResult rr = resolvePath(path);

      if (rr.node instanceof MountpointNode)
      {
         ((MountpointNode) rr.node).getFilesystem().write(rr.path, buf, offset);
         return;
      }

      if (rr.node instanceof FileNode)
      {
         ((FileNode) rr.node).read(buf, offset);
         return;
      }

      throw new FuseException("Not a File").initErrno(FuseException.EINVAL);
   }


   //
   // private methods

   private ResolveResult resolveParentPath(String path) throws FuseException
   {
      return resolvePath(path, true);
   }

   private ResolveResult resolvePath(String path) throws FuseException
   {
      return resolvePath(path, false);
   }

   private ResolveResult resolvePath(String path, boolean resolveParent) throws FuseException
   {
      Node node = rootNode;
      int i = 0;
      int subPathStart = 0;

      while (i < path.length() && !(node instanceof MountpointNode))
      {
         while (i < path.length() && path.charAt(i) == '/')
         {
            subPathStart = i;
            i++;
         }

         if (i >= path.length()) break;

         int nameStart = i;

         while (i < path.length() && path.charAt(i) != '/')
            i++;

         // are we resolving parent node & have just parsed the last component of path?
         if (resolveParent && i >= path.length()) break;

         String name = path.substring(nameStart, i);

         if (name.equals("."))
         {
            // same node
         }
         else if (name.equals(".."))
         {
            // parent node
            Node parentNode = node.getParent();
            if (parentNode != null)
               node = parentNode;
         }
         else
         {
            // child node
            Node childNode = null;
            if (node instanceof DirectoryNode && (childNode = ((DirectoryNode) node).getChild(name)) != null)
               node = childNode;
            else
               throw new FuseException("No such node").initErrno(FuseException.ENOENT);
         }

         subPathStart = i;
      }

      String subPath = path.substring(subPathStart);
      if (subPath.length() == 0 && node instanceof MountpointNode)
         subPath = "/";

      return new ResolveResult(subPath, node);
   }

   static class ResolveResult
   {
      String path;
      Node node;

      ResolveResult(String path, Node node)
      {
         this.path = path;
         this.node = node;
      }
   }
}
