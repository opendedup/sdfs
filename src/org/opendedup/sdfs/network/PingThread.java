package org.opendedup.sdfs.network;

import org.opendedup.sdfs.Main;

public class PingThread implements Runnable {
	HashClient client;

	public PingThread(HashClient client) {
		this.client = client;
		Thread th = new Thread(this);
		th.start();

	}

	@Override
	public void run() {
		while (!client.isClosed()) {
			try {
				client.ping();
				try {
					Thread.sleep(Main.PING_TIME);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			}
		}
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!");

	}

}
