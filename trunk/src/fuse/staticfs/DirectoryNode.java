/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.staticfs;

import fuse.FuseFtype;
import fuse.compat.FuseStat;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DirectoryNode extends Node {
	private Map<String, Node> children;

	public DirectoryNode(String name) {
		super(name);

		children = new HashMap<String, Node>();
	}

	//
	// create initial FuseStat structure (called from Node's constructor)

	protected FuseStat createStat() {
		FuseStat stat = new FuseStat();

		stat.mode = FuseFtype.TYPE_DIR | 0755;
		stat.uid = stat.gid = 0;
		stat.ctime = stat.mtime = stat.atime = (int) (System
				.currentTimeMillis() / 1000L);
		stat.size = 0;
		stat.blocks = 0;

		return stat;
	}

	//
	// public API

	public synchronized Node addChild(Node node) {
		Node previousNode = (Node) children.put(node.getName(), node);
		node.setParent(this);

		FuseStat stat = (FuseStat) getStat().clone();
		stat.mtime = stat.atime = (int) (System.currentTimeMillis() / 1000L);
		setStat(stat);

		if (previousNode != null) {
			previousNode.setParent(null);
		}

		return previousNode;
	}

	public synchronized Node removeChild(String name) {
		Node removedNode = (Node) children.remove(name);

		if (removedNode != null) {
			FuseStat stat = (FuseStat) getStat().clone();
			stat.mtime = stat.atime = (int) (System.currentTimeMillis() / 1000L);
			setStat(stat);

			removedNode.setParent(null);
		}

		return removedNode;
	}

	public synchronized Node getChild(String name) {
		return (Node) children.get(name);
	}

	public synchronized Node[] getChildren() {
		Collection<Node> childNodes = children.values();
		return (Node[]) childNodes.toArray(new Node[childNodes.size()]);
	}
}
