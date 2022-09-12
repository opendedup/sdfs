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

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;

public class ReplicationImportEvent extends SDFSEvent {

	private static final long serialVersionUID = 1L;
	public String src;
	public String dst;
	public String url;
	public long volumeid;
	public boolean canceled;
	public boolean paused;
	public long pausets;

	public ReplicationImportEvent(String src, String dst, String url, long volumeid) {
		super(MIMPORT, dst, "Importing " + src + " from " + url + " with volumeid " +
				volumeid + " to " + dst, SDFSEvent.INFO);
		this.src = src;
		this.dst = dst;
		this.url = url;
		this.volumeid = volumeid;
	}

	public ReplicationImportEvent(org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt) {
		super(evt);
		this.src = evt.getAttributesMap().get("src");
		this.dst = evt.getAttributesMap().get("dst");
		this.url = evt.getAttributesMap().get("url");
		this.volumeid = Long.parseLong(evt.getAttributesMap().get("volumeid"));
		this.paused = Boolean.parseBoolean(evt.getAttributesMap().get("paused"));
		this.pausets = Long.parseLong(evt.getAttributesMap().get("pausets"));
	}

	public void cancel() {
		this.canceled = true;
		this.endEvent("Replication Import Canceled", SDFSEvent.WARN);
	}

	public void pause(boolean pause) {
		this.paused = pause;
		if (this.paused) {
			this.shortMsg = "Replication Import Paused at " + System.currentTimeMillis();
		}
		if (this.paused) {
			this.shortMsg = "Replication Import Restarted at " + System.currentTimeMillis();
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
		b.putAttributes("volumeid", Long.toString(this.volumeid));
		b.putAttributes("paused", Boolean.toString(this.paused));
		b.putAttributes("pausets", Long.toString(this.pausets));
		return b.build();
	}

	@Override
	public Element toXML() throws ParserConfigurationException {
		Element el = super.toXML();
		el.setAttribute("src", src);
		el.setAttribute("dst", dst);
		el.setAttribute("url", url);
		el.setAttribute("volumeid",
				Long.toString(this.volumeid));
		el.setAttribute("pausets",
				Long.toString(this.pausets));
		el.setAttribute("paused",
				Boolean.toString(this.paused));
		return el;
	}

}
