package org.ziniki.couch.acdtx;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class AllDoneLatch {
	private int requests = 0;
	private int completed = 0;
	
	public synchronized void another() {
		requests++;
	}

	public synchronized void done() {
		completed++;
		this.notify();
	}

	public boolean await(int amt, TimeUnit unit) {
		long waitms = TimeUnit.MILLISECONDS.convert(amt, unit);
		long until = new Date().getTime()+waitms;
		synchronized (this) {
			while (requests > completed && new Date().getTime() < until) {
				try {
					long waitFor = until-new Date().getTime();
					this.wait(waitFor);
				} catch (InterruptedException ex) {
					// ignore this
				}
			}
			return requests == completed;
		}
	}
	
	@Override
	public String toString() {
		return "AllDoneLatch[" + completed + "/" + requests + "]";
	}
}
