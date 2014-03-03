package org.rabinfingerprint.scanner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.rabinfingerprint.handprint.Handprint;
import org.rabinfingerprint.handprint.Handprints;
import org.rabinfingerprint.handprint.Handprints.HandPrintFactory;
import org.rabinfingerprint.polynomial.Polynomial;

public class MatchModel {

	public static abstract class Match {
		protected final Handprint a, b;

		private Match(Handprint a, Handprint b) {
			this.a = a;
			this.b = b;
		}

		public Handprint getHandA() {
			return a;
		}

		public Handprint getHandB() {
			return b;
		}

		public abstract double getSimilarity();
	}

	public static class ExactMatch extends Match {
		private ExactMatch(Handprint a, Handprint b) {
			super(a, b);
		}

		@Override
		public double getSimilarity() {
			return 1.0;
		}
	}

	public static class PartialMatch extends Match {
		protected Double similarity;

		private PartialMatch(Handprint a, Handprint b) {
			super(a, b);
		}

		@Override
		public double getSimilarity() {
			if (similarity == null)
				similarity = a.getSimilarity(b);
			return similarity;
		}
	}

	public static class NonMatch extends Match {
		private NonMatch(Handprint a, Handprint b) {
			super(a, b);
		}

		@Override
		public double getSimilarity() {
			return 0.0;
		}
	}

	protected List<Match> matches = new ArrayList<Match>();

	public void getMatches(String pathA, String pathB) throws FileNotFoundException {
		final Polynomial p = Polynomial.createIrreducible(53);

		Collection<Handprint> handsA = getHandsFromPath(p, pathA);
		Collection<Handprint> handsB = getHandsFromPath(p, pathB);

		findExactMatches(handsA, handsB);
		findPartialMatches(handsA, handsB);
		findNonMatches(handsA, handsB);
	}

	private void findExactMatches(Collection<Handprint> handsA, Collection<Handprint> handsB) {
		System.out.println("thumbprinting " + (handsA.size() + handsB.size()) + " files");

		// thumbprint files
		TreeMap<Long, Handprint> thumbMapA = new TreeMap<Long, Handprint>();
		TreeMap<Long, Handprint> thumbMapB = new TreeMap<Long, Handprint>();

		thumbprintTasks(handsA, handsB, thumbMapA, thumbMapB);
		System.out.print("\n");

		List<Long> thumbsA = new ArrayList<Long>(thumbMapA.keySet());

		// print intersection
		for (Long thumb : thumbsA) {
			if (thumbMapB.containsKey(thumb)) {
				Handprint matchA = thumbMapA.get(thumb);
				Handprint matchB = thumbMapB.get(thumb);

				// found exact match
				handsA.remove(matchA);
				handsB.remove(matchB);

				matches.add(new ExactMatch(matchA, matchB));

				StringBuffer str = new StringBuffer();
				str.append("Found exact match between ");
				// str.append(matchA.getFile().toString());
				str.append(" and ");
				// str.append(matchB.getFile().toString());
				System.out.println(str.toString());
			}
		}
	}

	private void thumbprintTasks(final Collection<Handprint> handsA,
			final Collection<Handprint> handsB, final TreeMap<Long, Handprint> thumbMapA,
			final TreeMap<Long, Handprint> thumbMapB) {

		final CountDownLatch doneSignal = new CountDownLatch(2);
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final class ThumbRunnable implements Runnable {
			private final Collection<Handprint> hands;
			private final TreeMap<Long, Handprint> map;

			public ThumbRunnable(Collection<Handprint> hands, TreeMap<Long, Handprint> map) {
				super();
				this.hands = hands;
				this.map = map;
			}

			public void run() {
				for (Handprint hand : hands) {
					map.put(hand.getPalm(), hand);
					System.out.print(".");
					System.out.flush();
				}
				doneSignal.countDown();
			}
		}

		executor.execute(new ThumbRunnable(handsA, thumbMapA));
		executor.execute(new ThumbRunnable(handsB, thumbMapB));

		try {
			doneSignal.await(); // wait for all to finish
		} catch (InterruptedException ie) {
		}
		executor.shutdown();

	}

	private void findPartialMatches(Collection<Handprint> handsA, Collection<Handprint> handsB) {
		System.out.println("handprinting " + (handsA.size() + handsB.size()) + " files");

		// build all fingers
		TreeMap<Long, Handprint> handMapA = new TreeMap<Long, Handprint>();
		TreeMap<Long, Handprint> handMapB = new TreeMap<Long, Handprint>();

		handprintTasks(handsA, handsB, handMapA, handMapB);

		// print intersection
		List<Long> fingersA = new ArrayList<Long>(handMapA.keySet());
		for (Long finger : fingersA) {
			if (handMapB.containsKey(finger)) {
				Handprint matchA = handMapA.get(finger);
				Handprint matchB = handMapB.get(finger);

				// found partial match
				handsA.remove(matchA);
				handsB.remove(matchB);

				matches.add(new PartialMatch(matchA, matchB));

				StringBuffer str = new StringBuffer();
				str.append("Found partial match between ");
				//str.append(matchA.getFile().toString());
				str.append(" and ");
				//str.append(matchB.getFile().toString());
				str.append(" with similarity " + (100.0 * matchA.getSimilarity(matchB)));
				System.out.println(str.toString());
			}
		}
	}

	private void handprintTasks(Collection<Handprint> handsA, Collection<Handprint> handsB,
			TreeMap<Long, Handprint> handMapA, TreeMap<Long, Handprint> handMapB) {

		final CountDownLatch doneSignal = new CountDownLatch(2);
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final class HandRunnable implements Runnable {
			private final Collection<Handprint> hands;
			private final TreeMap<Long, Handprint> map;

			public HandRunnable(Collection<Handprint> hands, TreeMap<Long, Handprint> map) {
				super();
				this.hands = hands;
				this.map = map;
			}

			public void run() {
				for (Handprint hand : hands) {
					for (Long finger : hand.getHandFingers().keySet()) {
						map.put(finger, hand);
					}
					System.out.print(".");
					System.out.flush();
				}
				doneSignal.countDown();
			}
		}

		executor.execute(new HandRunnable(handsA, handMapA));
		executor.execute(new HandRunnable(handsB, handMapB));

		try {
			doneSignal.await(); // wait for all to finish
		} catch (InterruptedException ie) {
		}
		executor.shutdown();
	}

	private void findNonMatches(Collection<Handprint> handsA, Collection<Handprint> handsB) {
		for (Handprint hand : handsA) {
			matches.add(new NonMatch(hand, null));

			StringBuffer str = new StringBuffer();
			str.append("Found no match for ");
			//str.append(hand.getFile().toString());
			System.out.println(str.toString());
		}

		for (Handprint hand : handsB) {
			matches.add(new NonMatch(null, hand));

			StringBuffer str = new StringBuffer();
			str.append("Found no match for ");
			//str.append(hand.getFile().toString());
			System.out.println(str.toString());
		}
	}

	private static Collection<Handprint> getHandsFromPath(final Polynomial p, final String path)
			throws FileNotFoundException {
		File dir = new File(path);
		List<File> files = FileListing.getFileListing(dir);
		List<Handprint> hands = new ArrayList<Handprint>();
		HandPrintFactory factory = Handprints.newFactory(p);
		for (File file : files) {
			if (!file.isFile())
				continue;
			hands.add(factory.newHandprint(new FileInputStream(file)));
		}

		return hands;
	}
}
