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

public class BlockImportEvent extends SDFSEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public long blocksImported;
	public long bytesImported;
	public long filesImported;
	public long virtualDataImported;

	protected BlockImportEvent(String target, String shortMsg, Level level) {
		super(MIMPORT, target, shortMsg, level);
	}

	@Override
    public org.opendedup.grpc.SDFSEvent toProtoBuf() throws IOException {
        org.opendedup.grpc.SDFSEvent evt = super.toProtoBuf();
        org.opendedup.grpc.SDFSEvent.Builder b= org.opendedup.grpc.SDFSEvent.newBuilder(evt);
		b.putAttributes("blocks-imported", Long.toString(this.blocksImported));
		b.putAttributes("bytes-imported", Long.toString(this.bytesImported));
		b.putAttributes("files-imported", Long.toString(this.filesImported));
		b.putAttributes("virtual-data-imported", Long.toString(this.virtualDataImported));
        return b.build();
    }

	@Override
	public Element toXML() throws ParserConfigurationException {
		Element el = super.toXML();
		el.setAttribute("blocks-imported", Long.toString(this.blocksImported));
		el.setAttribute("bytes-imported", Long.toString(this.bytesImported));
		el.setAttribute("files-imported", Long.toString(this.filesImported));
		el.setAttribute("virtual-data-imported",
				Long.toString(this.virtualDataImported));
		return el;
	}

}
