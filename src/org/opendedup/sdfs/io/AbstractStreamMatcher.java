package org.opendedup.sdfs.io;

import java.util.List;

import org.opendedup.hashing.Finger;
import org.simpleframework.http.Response;
import org.simpleframework.http.socket.Session;
import org.simpleframework.http.socket.service.Service;
import org.w3c.dom.Element;

import com.google.common.eventbus.EventBus;

public abstract class AbstractStreamMatcher implements Service{
	public abstract boolean isMatch(String file,List<Finger>fs) throws MatchException;
	public abstract String getWSPath();
	public abstract String getWPath();
	public abstract void getResult(String srcfile, long start, Response response)throws Exception;
	@Override
	public abstract void connect(Session arg0);
	public abstract void initialize(Element config);
	public abstract void start();
	private static EventBus eventUploadBus = new EventBus();
	public void registerEvents(Object obj) {
		eventUploadBus.register(obj);
	}
	
	public EventBus getEventBus() {
		return eventUploadBus;
	}
}
