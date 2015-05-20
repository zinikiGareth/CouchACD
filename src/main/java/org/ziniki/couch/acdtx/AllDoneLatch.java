package org.ziniki.couch.acdtx;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscriber;

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

	public Observable<Boolean> onReleased(int amt, TimeUnit unit) {
		return Observable.create(new Observable.OnSubscribe<Boolean>() {
			@Override
			public void call(Subscriber<? super Boolean> observer) {
				boolean done = AllDoneLatch.this.await(amt, unit);
				if (!observer.isUnsubscribed()) {
					observer.onNext(done);
					observer.onCompleted();
				}
			}
		});
	}
	
	@Override
	public String toString() {
		return "AllDoneLatch[" + completed + "/" + requests + "]";
	}
}
