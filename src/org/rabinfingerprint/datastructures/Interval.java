package org.rabinfingerprint.datastructures;

import java.util.Comparator;

/**
 * A Parameter-Style Object that contains a start and end index.
 * 
 * The indices are in common set notation where the start index is inclusive and
 * the end offset is exclusive. This allows us to easily represent zero-width
 * intervals -- in this case, anything where start == end;
 * 
 * The default comparator sorts first be the start index, then by the end index.
 * 
 */
public class Interval implements Comparable<Interval> {

	private final Long start;
	private final Long end;
	private byte [] chunk;
	/**
	 * The default comparator. Sorts first be the start index, then by the end
	 * index.
	 */
	public static final Comparator<Interval> START_END_COMPARATOR = new Comparator<Interval>() {
		public int compare(Interval o1, Interval o2) {
			if (o1 == o2)
				return 0;
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;
			int cmp = o1.start.compareTo(o2.start);
			if (cmp != 0)
				return cmp;
			return o1.end.compareTo(o2.end);
		}
	};

	/**
	 * This comparator is used for comparing intervals in
	 * {@link FastSentenceParagraphInfo}.
	 */
	public static final Comparator<Interval> START_END_INV_COMPARATOR = new Comparator<Interval>() {
		public int compare(Interval o1, Interval o2) {
			if (o1 == o2)
				return 0;
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;
			int cmp = o1.start.compareTo(o2.start);
			if (cmp != 0)
				return cmp;
			return o2.end.compareTo(o1.end);
		}
	};

	public static Interval createUndirected(Long start, Long end) {
		if (start.compareTo(end) > 0)
			return new Interval(end, start);
		return new Interval(start, end);
	}

	public Interval(Long start, Long end) {
		if (start == null || end == null)
			throw new IllegalArgumentException("Interval indeces cannot be null");
		if (start.compareTo(end) > 0)
			throw new IllegalArgumentException("Interval indeces out of order");

		this.start = start;
		this.end = end;
	}
	
	public Interval(Long start, Long end, byte [] chunk) {
		if (start == null || end == null)
			throw new IllegalArgumentException("Interval indeces cannot be null");
		if (start.compareTo(end) > 0)
			throw new IllegalArgumentException("Interval indeces out of order");

		this.start = start;
		this.end = end;
		this.chunk = chunk;
	}

	/**
	 * Returns the inclusive start offset
	 */
	public Long getStart() {
		return start;
	}
	
	public byte [] getChunk() {
		return this.chunk;
	}

	/**
	 * Returns the exclusive end offset
	 */
	public Long getEnd() {
		return end;
	}

	public Long getSize() {
		return end - start;
	}

	/**
	 * Return the overlapping region of this interval and the input. If the
	 * intervals do not overlap, null is returned.
	 */
	public Interval intersection(Interval interval) {
		Long istart = interval.getStart();
		Long iend = interval.getEnd();
		if (istart >= end || start >= iend)
			return null; // no overlap
		return new Interval(Math.max(start, istart), Math.min(end, iend));
	}

	/**
	 * Returns the smallest interval that contains both this interval and the
	 * input. Note that this not a strict union since indices not included in
	 * either interval can be included in resulting interval.
	 */
	public Interval union(Interval interval) {
		Long istart = interval.getStart();
		Long iend = interval.getEnd();
		return new Interval(Math.min(start, istart), Math.max(end, iend));
	}

	/**
	 * Tests whether this is an empty (a.k.a. zero-length) interval
	 */
	public boolean isEmpty() {
		return start == end;
	}

	/**
	 * Tests whether the input interval overlaps this interval. Adjacency does
	 * not count as overlapping.
	 */
	public boolean isOverlap(Interval interval) {
		if (interval.start >= this.end)
			return false;
		if (this.start >= interval.end)
			return false;
		return true;
	}

	/**
	 * Tests whether this interval completely contains the input interval.
	 */
	public boolean contains(Interval interval) {
		return (this.start <= interval.start && this.end >= interval.end);
	}

	/**
	 * Tests whether this interval contains the input index.
	 */
	public boolean contains(Long index) {
		return (this.start <= index && this.end > index);
	}

	/**
	 * Object override for printing
	 */
	@Override
	public String toString() {
		return "[" + start + ", " + end + ")";
	}

	/**
	 * Comparable<Interval> Implementation
	 */
	public int compareTo(Interval o) {
		return START_END_COMPARATOR.compare(this, o);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Interval other = (Interval) obj;
		if (end == null) {
			if (other.end != null)
				return false;
		} else if (!end.equals(other.end))
			return false;
		if (start == null) {
			if (other.start != null)
				return false;
		} else if (!start.equals(other.start))
			return false;
		return true;
	}
}
