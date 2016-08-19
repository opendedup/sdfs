/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.ClusterSocket;

public class StopGCMasterCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	private Address gcMaster = null;

	public StopGCMasterCmd() {
		opts = new RequestOptions(ResponseMode.GET_ALL, 0);

	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.STOP_GC_MASTER_CMD);
		try {
			RspList<Object> lst = soc.getDispatcher().castMessage(null,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"STOP_GC_MASTER_CMD Exception thrown for "
									+ rsp.getSender());
					// throw rsp.getException();
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"STOP_GC_MASTER_CMD Host unreachable Exception thrown for "
									+ rsp.getSender());
					// throw new
					// IOException("FIND_GC_MASTER_CMD Host unreachable Exception thrown for "
					// + rsp.getSender());
				} else {
					if (rsp.getValue() != null) {
						SDFSLogger.getLog().debug(
								"STOP_GC_MASTER_CMD completed for "
										+ rsp.getSender() + " returned="
										+ rsp.getValue());
						boolean m = (Boolean) rsp.getValue();
						if (m) {
							if (this.gcMaster != null)
								throw new IOException(
										"STOP_GC_MASTER_CMD already identified at ["
												+ gcMaster.toString()
												+ "] but has also been identified at ["
												+ rsp.getSender() + "].");
							else
								this.gcMaster = rsp.getSender();
						}

					}
				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running STOP_GC_MASTER_CMD",
					e);
			throw new IOException(e);
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.STOP_GC_MASTER_CMD;
	}

	public Address getResults() {
		return this.gcMaster;
	}

}
