package org.opendedup.hashing;

public interface AbstractPoolThread {
	public void start();

	public void exit();

	public boolean isStopped();

}