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

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;

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
	public Element toXML() throws ParserConfigurationException {
		Element el = super.toXML();
		el.setAttribute("current-size", Long.toString(this.currentSz));
		el.setAttribute("max-size", Long.toString(this.maxSz));
		el.setAttribute("dse-size", Long.toString(this.dseSz));
		el.setAttribute("dse-max-size", Long.toString(this.maxDseSz));
		el.setAttribute("disk-usage", Long.toString(this.dskUsage));
		el.setAttribute("max-disk-usage", Long.toString(this.maxDskUsage));
		return el;
	}

}
