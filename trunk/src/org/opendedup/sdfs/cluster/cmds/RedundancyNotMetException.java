package org.opendedup.sdfs.cluster.cmds;

public class RedundancyNotMetException extends ClusterCmdException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2523934501123942317L;
	public byte[] hashloc = null;

	public RedundancyNotMetException(int written, int requirement,
			byte[] hashloc) {
		super("Redundancy Requirement not met [" + written
				+ "] copies written and [" + requirement + "] copies required.");
		this.hashloc = hashloc;
	}

}
