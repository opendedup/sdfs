package org.opendedup.logging;

public class JSONLayoutException extends RuntimeException{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public JSONLayoutException(String message, Throwable t){
        super(message,t);
    }

    public JSONLayoutException(Throwable t){
        super(t);
    }
}
