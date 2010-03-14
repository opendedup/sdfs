/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.zipfs.util;


public class Tree
{
   Node rootNode;

   public Tree()
   {
      rootNode = new Node();
      rootNode.setName("$ROOT");
      rootNode.setParent(rootNode);
   }

   public void addNode(String path, Object value)
   {
      Node node = rootNode;
      String[] pathParts = path.split("/");

      for (int i = 0; i < pathParts.length; i++)
      {
         String pathPart = pathParts[i];
         if (pathPart.equals("") || pathPart.equals("."))
         {
            // the same node
         }
         else if (pathPart.equals(".."))
         {
            // parent node
            node = node.getParent();
         }
         else
         {
            Node childNode = node.getChild(pathPart);
            if (childNode == null)
            {
               childNode = new Node();
               childNode.setName(pathPart);
               childNode.setParent(node);
               node.addChild(childNode);
            }
            node = childNode;
         }
      }

      node.setValue(value);
   }

   public Node lookupNode(String path)
   {
      Node node = rootNode;
      String[] pathParts = path.split("/");

      for (int i = 0; i < pathParts.length; i++)
      {
         String pathPart = pathParts[i];
         if (pathPart.equals("") || pathPart.equals("."))
         {
            // the same node
         }
         else if (pathPart.equals(".."))
         {
            // parent node
            node = node.getParent();
         }
         else
         {
            node = node.getChild(pathPart);
            if (node == null)
               break;
         }
      }

      return node;
   }

}
