package org.opendedup.buse.driver;

public class ShutdownHook extends Thread {
	public String dev;
	public BUSE bClass;

	ShutdownHook(String dev,BUSE bClass) {
		this.bClass = bClass;
		this.dev = dev;
		System.out.println("Registered shutdown hook for " + dev);
	}

	@Override
	public void run() {
		System.out.println("#### Shutting down dev " + dev + " ####");

		try {
			BUSEMkDev.closeDev(dev);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bClass.close();
		System.out.println("Shut Down " + dev);
	}
}
