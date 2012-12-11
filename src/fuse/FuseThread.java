package fuse;

public class FuseThread implements Runnable {
	Thread origTh = null;
	private boolean closed = false;
	private static ThreadGroup destroyedThreads = new ThreadGroup("destroyed");

	public FuseThread() {
		this.origTh = Thread.currentThread();
		System.out.println("Init Thread " + this.origTh.getName() + " "
				+ this.origTh.isAlive());
		Thread th = new Thread(this);
		th.start();

	}

	public native void clearThread();

	@Override
	public void run() {
		System.out.println("Starting Thread " + this.origTh.getName());
		long dur = 5 * 1000;
		while (!closed) {
			try {
				this.origTh.join(1000);
				long ltime = Long.parseLong(this.origTh.getName()) + dur;
				long tm = System.currentTimeMillis();
				if (ltime < tm) {
					System.out.println("clearing thread "
							+ this.origTh.getName());
					this.origTh.setName("OldThread-" + tm);
					Thread th = new Thread(this.origTh) {
						@Override
						public void run() {
							System.out.println("Stopped");
						}
					};
					th.start();

					this.closed = true;
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
