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
package org.opendedup.util;

import java.io.IOException;
import java.net.ServerSocket;

public class FindOpenPort {

	public static int pickFreePort(int start)

	{
		int port = -1;
		while (port == -1) {
			ServerSocket socket = null;

			try

			{

				socket = new ServerSocket(start);

				port = socket.getLocalPort();

			}

			catch (IOException e)

			{

			}

			finally

			{

				if (socket != null)

				{

					try

					{

						socket.close();

					}

					catch (IOException e)

					{

					}

				}

			}
			start++;
		}
		return port;

	}

	public static void main(String[] args) {
		System.out.println(pickFreePort(6442));
	}

}
