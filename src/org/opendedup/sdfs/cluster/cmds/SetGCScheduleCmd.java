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

import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.ClusterSocket;

public class SetGCScheduleCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	// private ArrayList<String> results = new ArrayList<String>();
	private String schedule;

	public SetGCScheduleCmd(String schedule) {
		this.schedule = schedule;
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout);

	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] vb = this.schedule.getBytes();
		byte[] b = new byte[1 + 4 + vb.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.SET_GC_SCHEDULE);
		buf.putInt(vb.length);
		buf.put(vb);
		try {
			RspList<Object> lst = soc.getDispatcher().castMessage(null,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"Set Schedule Exception thrown for "
									+ rsp.getSender());
					throw rsp.getException();
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"Set Schedule  unreachable for " + rsp.getSender());
				}

			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running set schedule", e);
			throw new IOException(e);
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.SET_GC_SCHEDULE;
	}

}
