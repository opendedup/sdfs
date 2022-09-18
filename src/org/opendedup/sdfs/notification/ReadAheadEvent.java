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


import org.opendedup.sdfs.io.MetaDataDedupFile;

public class ReadAheadEvent extends SDFSEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public MetaDataDedupFile mf;
	public boolean running = false;

	public ReadAheadEvent(String target,MetaDataDedupFile mf) {
		super(RAE, target, "Caching " +mf.getPath(), SDFSEvent.INFO);
		this.mf = mf;
		this.running = true;
	}
	
	public void cancelEvent() {
		this.running = false;
	}

	

	@Override
    public org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent toProtoBuf() throws IOException {
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent evt = super.toProtoBuf();
        org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.Builder b= org.opendedup.grpc.SDFSEventOuterClass.SDFSEvent.newBuilder(evt);
		b.putAttributes("file", mf.getPath());
        return b.build();
    }

}
