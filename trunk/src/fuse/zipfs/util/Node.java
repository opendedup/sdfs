/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.zipfs.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Node
{
   private String name;
   private Node parent;
   private Object value;
   private Map<String, Node> children;

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public Node getParent()
   {
      return parent;
   }

   public void setParent(Node parent)
   {
      this.parent = parent;
   }

   public Object getValue()
   {
      return value;
   }

   public void setValue(Object value)
   {
      this.value = value;
   }

   public boolean isLeafNode()
   {
      return children == null || children.size() == 0;
   }

   public void addChild(Node node)
   {
      if (children == null)
         children = new HashMap<String, Node>();

      children.put(node.getName(), node);
   }

   public Node getChild(String name)
   {
      return (children == null)? null : (Node)children.get(name);
   }

   public Collection<?> getChildren()
   {
      return (children == null)? Collections.EMPTY_LIST : children.values();
   }
}
