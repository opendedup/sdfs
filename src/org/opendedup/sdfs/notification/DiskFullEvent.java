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

public class DiskFullEvent extends SDFSEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public long currentSz;
	public long maxSz;
	public long dseSz;
	public long maxDseSz;
	public long dskUsage;
	public long maxDskUsage;

	public DiskFullEvent(String shortMsg) {
		super(DSKFL, getTarget(), shortMsg, SDFSEvent.ERROR);
	}

	@Override
    public org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent toProtoBuf() throws IOException{
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt = super.toProtoBuf();
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.Builder b= org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.newBuilder(evt);
		b.putAttributes("current-size", Long.toString(this.currentSz));
		b.putAttributes("max-size", Long.toString(this.maxSz));
		b.putAttributes("dse-size", Long.toString(this.dseSz));
		b.putAttributes("dse-max-size", Long.toString(this.maxDseSz));
		b.putAttributes("disk-usage", Long.toString(this.dskUsage));
		b.putAttributes("max-disk-usage", Long.toString(this.maxDskUsage));
        return b.build();
    }

}
