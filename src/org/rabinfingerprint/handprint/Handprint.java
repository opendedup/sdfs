package org.rabinfingerprint.handprint;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.TreeSet;

import org.rabinfingerprint.datastructures.Interval;
import org.rabinfingerprint.handprint.FingerFactory.ChunkVisitor;
import org.rabinfingerprint.handprint.Handprints.HandprintException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class Handprint {
	private final InputStream stream;
	private final FingerFactory factory;
	private final int fingersPerHand;

	private Long palm;
	private Multimap<Long, Interval> fingers;
	private Multimap<Long, Interval> hand;

	public Handprint(InputStream stream, int fingersPerHand, FingerFactory factory) {
		this.stream = stream;
		this.factory = factory;
		this.fingersPerHand = fingersPerHand;
	}

	public void buildAll() {
		getPalm();
		getAllFingers();
		getHandFingers();
	}

	public Long getPalm() {
		if (palm != null)
			return palm;
		try {
			palm = factory.getFullFingerprint(stream);
		} catch (IOException e) {
			throw new HandprintException("Error while computing fingerprints", e);
		}
		return palm;
	}

	public Multimap<Long, Interval> getAllFingers() {
		if (fingers != null)
			return fingers;
		try {
			fingers = ArrayListMultimap.create();
			factory.getChunkFingerprints(stream, new ChunkVisitor() {
				public void visit(long fingerprint, long chunkStart, long chunkEnd) {
					fingers.put(fingerprint, new Interval(chunkStart, chunkEnd));
				}
			});
		} catch (IOException e) {
			throw new HandprintException("Error while computing fingerprints", e);
		}
		return fingers;
	}
	
	public static final Comparator<Long> REVERSE_LONG_SORT = new Comparator<Long>() {
		public int compare(Long o1, Long o2) {
			return o2.compareTo(o1);
		}
	};

	public Multimap<Long, Interval> getHandFingers() {
		if (hand != null)
			return hand;
		hand = ArrayListMultimap.create();
		Multimap<Long, Interval> all = getAllFingers();
		TreeSet<Long> keys = Sets.newTreeSet(REVERSE_LONG_SORT);
		keys.addAll(all.keySet());
		for (Long key : Iterables.limit(keys, fingersPerHand)) {
			hand.putAll(key, all.get(key));
		}
		return hand;
	}

	public int getFingerCount() {
		return getAllFingers().size();
	}

	public int getIntersectingFingerCount(Handprint other) {
		return Sets.intersection(getAllFingers().keySet(), other.getAllFingers().keySet()).size();
	}

	public double getSimilarity(Handprint other) {
		int maxFingers = Math.max(getFingerCount(), other.getFingerCount());
		if (maxFingers == 0) {
			return 1.0;
		}
		return (double) getIntersectingFingerCount(other) / (double) maxFingers;
	}

	@Override
	public String toString() {
		return getHandFingers().toString();
	}
}
