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
package org.opendedup.sdfs.notification;

import java.io.IOException;


public class ReplicationImportEvent extends SDFSEvent {

	private static final long serialVersionUID = 1L;
	public String src;
	public String dst;
	public String url;
	public long volumeid;
	public boolean canceled;
	public boolean paused;
	public long pausets;
	public boolean mtls;
	public boolean onDemand;
	public long fileSize;
	public long bytesImported;
	public long bytesProcessed;

	public ReplicationImportEvent(String src, String dst, String url, long volumeid,boolean mtls,boolean onDemand) {
		super(IMPORT, dst, "Importing " + src + " from " + url + " with volumeid " +
				volumeid + " to " + dst, SDFSEvent.INFO);
		this.src = src;
		this.dst = dst;
		this.url = url;
		this.mtls = mtls;
		this.volumeid = volumeid;
		this.onDemand = onDemand;
	}

	public ReplicationImportEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt) {
		super(evt);
		this.src = evt.getAttributesMap().get("src");
		this.dst = evt.getAttributesMap().get("dst");
		this.url = evt.getAttributesMap().get("url");
		this.mtls = Boolean.parseBoolean(evt.getAttributesMap().get("mtls"));
		this.volumeid = Long.parseLong(evt.getAttributesMap().get("volumeid"));
		this.paused = Boolean.parseBoolean(evt.getAttributesMap().get("paused"));
		this.pausets = Long.parseLong(evt.getAttributesMap().get("pausets"));
		this.onDemand = Boolean.parseBoolean(evt.getAttributesMap().get("onDemand"));
		this.bytesImported =Long.parseLong(evt.getAttributesMap().get("bytesimported"));
		this.fileSize =Long.parseLong(evt.getAttributesMap().get("filesize"));
		this.bytesProcessed =Long.parseLong(evt.getAttributesMap().get("bytesprocessed"));
	}

	public void cancel() {
		this.canceled = true;
		this.endEvent("Replication Import Canceled", SDFSEvent.WARN);
	}

	public void pause(boolean pause) {
		this.paused = pause;
		if (this.paused) {
			this.setShortMsg("Replication Import Paused at " + System.currentTimeMillis());
		}
		if (!this.paused) {
			this.setShortMsg("Replication Import Restarted at " + System.currentTimeMillis());
		}
	}

	@Override
	public org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent toProtoBuf() throws IOException {
		org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt = super.toProtoBuf();
		org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.Builder b = org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent
				.newBuilder(evt);
		b.putAttributes("src", src);
		b.putAttributes("dst", dst);
		b.putAttributes("url", url);
		b.putAttributes("mtls", Boolean.toString(mtls));
		b.putAttributes("volumeid", Long.toString(this.volumeid));
		b.putAttributes("paused", Boolean.toString(this.paused));
		b.putAttributes("pausets", Long.toString(this.pausets));
		b.putAttributes("onDemand", Boolean.toString(this.onDemand));
		b.putAttributes("bytesimported",  Long.toString(this.bytesImported));
		b.putAttributes("filesize",  Long.toString(this.fileSize));
		b.putAttributes("bytesprocessed",  Long.toString(this.bytesProcessed));
		return b.build();
	}

}