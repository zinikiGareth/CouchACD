package org.ziniki.couch.acdtx;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class AllDoneLatch {
	private int counter = 0;
	
	public synchronized void another() {
		counter++;
	}

	public synchronized void done() {
		counter--;
		this.notify();
	}

	public boolean await(int amt, TimeUnit unit) {
		long waitms = TimeUnit.MILLISECONDS.convert(amt, unit);
		long until = new Date().getTime()+waitms;
		synchronized (this) {
			while (counter > 0 && new Date().getTime() < until) {
				try {
					long waitFor = until-new Date().getTime();
					this.wait(waitFor);
				} catch (InterruptedException ex) {
					// ignore this
				}
			}
			return counter == 0;
		}
	}
}
