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
