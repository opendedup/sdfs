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
package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PageSizeCmd implements IOCmd {
	private int pageSize = -1;

	public PageSizeCmd() {
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.STORE_PAGE_SIZE);
		os.flush();
		this.pageSize = is.readInt();
	}

	public int pageSize() {
		return this.pageSize;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.STORE_PAGE_SIZE;
	}

	@Override
	public Integer getResult() {
		return this.pageSize;
	}

}
