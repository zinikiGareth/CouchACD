package org.ziniki.couch.acdtx;

import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscriber;

public class AllDoneLatch {
	public class Latch {
		private boolean released;
		private final String id;

		private Latch(String id) {
			this.id = id;
			this.released = false;
		}
		
		public void release() {
			synchronized (this) {
				if (!released) {
					AllDoneLatch.this.done(id);
					released = true;
				}
			}
		}
		
		@Override
		public String toString() {
			return "Latch[" + id + "]";
		}
	}

	private boolean allReleased;
	private int requests = 0;
	private int completed = 0;
	private Set<String> outstanding = new TreeSet<String>();
	
	public synchronized Latch another(String id) {
		requests++;
//		System.err.println(id + " another: " + completed + "/" + requests);
		/*
		if (id.endsWith("_G_24") || id.endsWith("_G_25"))
		try {
			throw new RuntimeException("latched " + id + ": " + completed + "/" + requests);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		*/
		if (allReleased) {
			try {
				throw new RuntimeException("Attempting to latch " + id + " after tx released");
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			System.err.println("Attempting to latch " + id + " after tx released");
			System.exit(12);
		}
		outstanding.add(id);
		return new Latch(id);
	}

	private synchronized void done(String id) {
		/*
		if (id.endsWith("_G_0"))
		try {
			throw new RuntimeException("released " + completed + "/" + requests);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		*/
		completed++;
//		System.err.println(id + " done: " + completed + "/" + requests);
		if (!outstanding.remove(id))
			throw new RuntimeException("Was not waiting for " + id + " have " + outstanding);
		if (completed > requests) {
			System.err.println("Attempting to complete when none outstanding");
			System.exit(12);
		}
		this.notify();
	}

	public boolean await(int amt, TimeUnit unit) {
		long waitms = TimeUnit.MILLISECONDS.convert(amt, unit);
		long until = new Date().getTime()+waitms;
		synchronized (this) {
//			try {
//				throw new RuntimeException("awaiting release: " + completed + "/" + requests);
//			} catch (Exception ex) {
//				ex.printStackTrace();
//			}
			while (requests > completed && new Date().getTime() < until) {
				try {
					long waitFor = until-new Date().getTime();
//					System.err.println("Waiting because " + requests + " > " + completed + " for " + waitFor);
					this.wait(waitFor);
//					System.err.println("notified: " + requests + " == " + completed);
				} catch (InterruptedException ex) {
					// ignore this
				}
			}
//			System.err.println("releasing with " + completed + "/" + requests + ": " + (completed == requests));
			this.allReleased = (requests == completed);
			if (!this.allReleased)
				System.out.println("Still waiting for " + this.outstanding);
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
