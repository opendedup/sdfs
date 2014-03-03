package org.rabinfingerprint.polynomial;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

/**
 * An immutable polynomial in the finite field GF(2^k)
 * 
 * Supports standard arithmetic in the field, as well as reducibility tests.
 */
public class Polynomial implements Arithmetic< Polynomial >, Comparable< Polynomial > {

	/** number of elements in the finite field GF(2^k) */
	public static final BigInteger Q = BigInteger.valueOf(2L);

	/** the polynomial "x" */
	public static final Polynomial X = Polynomial.createFromLong(2L);

	/** the polynomial "1" */
	public static final Polynomial ONE = Polynomial.createFromLong(1L);

	/** a reverse comparator so that polynomials are printed out correctly */
	private static final class ReverseComparator implements Comparator<BigInteger> {
		public int compare(BigInteger o1, BigInteger o2) {
			return -1 * o1.compareTo(o2);
		}
	}

	/**
	 * Constructs a polynomial using the bits from a long. Note that Java does
	 * not support unsigned longs.
	 */
	public static Polynomial createFromLong(long l) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		for (int i = 0; i < 64; i++) {
			if (((l >> i) & 1) == 1)
				dgrs.add(BigInteger.valueOf(i));
		}
		return new Polynomial(dgrs);
	}

	public static Polynomial createFromBytes(byte[] bytes) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		int degree = 0;
		for (int i = bytes.length - 1; i >= 0; i--) {
			for (int j = 0; j < 8; j++) {
				if ((((bytes[i] >> j) & 1) == 1)) {
					dgrs.add(BigInteger.valueOf(degree));
				}
				degree++;
			}
		}
		return new Polynomial(dgrs);
	}

	/**
	 * Constructs a polynomial using the bits from an array of bytes, limiting
	 * the degree to the specified size.
	 * 
	 * We set the final degree to ensure a monic polynomial of the correct
	 * degree.
	 */
	public static Polynomial createFromBytes(byte[] bytes, int degree) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		for (int i = 0; i < degree; i++) {
			if (Polynomials.getBit(bytes, i))
				dgrs.add(BigInteger.valueOf(i));
		}
		dgrs.add(BigInteger.valueOf(degree));
		return new Polynomial(dgrs);
	}

	/**
	 * Constructs a random polynomial of degree "degree"
	 */
	public static Polynomial createRandom(int degree) {
		Random random = new Random();
		byte[] bytes = new byte[(degree / 8) + 1];
		random.nextBytes(bytes);
		return createFromBytes(bytes, degree);
	}

	/**
	 * Finds a random irreducible polynomial of degree "degree"
	 */
	public static Polynomial createIrreducible(int degree) {
		while (true) {
			Polynomial p = createRandom(degree);
			if (p.getReducibility() == Reducibility.IRREDUCIBLE)
				return p;
		}
	}

	/**
	 * An enumeration representing the reducibility of the polynomial
	 * 
	 * A polynomial p(x) in GF(2^k) is called irreducible over GF[2^k] if it is
	 * non-constant and cannot be represented as the product of two or more
	 * non-constant polynomials from GF(2^k).
	 * 
	 * http://en.wikipedia.org/wiki/Irreducible_element
	 */
	public static enum Reducibility {
		REDUCIBLE, IRREDUCIBLE
	};

	/**
	 * A (sorted) set of the degrees of the terms of the polynomial. The
	 * sortedness helps quickly compute the degree as well as print out the
	 * terms in order. The O(nlogn) performance of insertions and deletions
	 * might actually hurt us, though, so we might consider moving to a HashSet
	 */
	private final TreeSet<BigInteger> degrees;

	/**
	 * Construct a new, empty polynomial
	 */
	public Polynomial() {
		this.degrees = createDegreesCollection();
	}

	/**
	 * Construct a new polynomial copy of the input argument
	 */
	public Polynomial(Polynomial p) {
		this(p.degrees);
	}

	/**
	 * Construct a new polynomial from a collection of degrees
	 */
	@SuppressWarnings("unchecked")
	protected Polynomial(TreeSet<BigInteger> degrees) {
		this.degrees = (TreeSet<BigInteger>) degrees.clone();
	}

	/**
	 * Factory for create the degrees collection.
	 */
	protected static TreeSet<BigInteger> createDegreesCollection() {
		return new TreeSet<BigInteger>(new ReverseComparator());
	}

	/**
	 * Factory for create the copy of current degrees collection.
	 */
	@SuppressWarnings("unchecked")
	protected TreeSet<BigInteger> createDegreesCollectionCopy() {
		return (TreeSet<BigInteger>) this.degrees.clone();
	}

	/**
	 * Returns the degree of the highest term or -1 otherwise.
	 */
	public BigInteger degree() {
		if (degrees.isEmpty())
			return BigInteger.ONE.negate();
		return degrees.first();
	}

	/**
	 * Tests if the polynomial is empty, i.e. it has no terms
	 */
	public boolean isEmpty() {
		return degrees.isEmpty();
	}

	/**
	 * Computes (this + that) in GF(2^k)
	 */
	public Polynomial add(Polynomial that) {
		return xor(that);
	}

	/**
	 * Computes (this - that) in GF(2^k)
	 */
	public Polynomial subtract(Polynomial that) {
		return xor(that);
	}

	/**
	 * Computes (this * that) in GF(2^k)
	 */
	public Polynomial multiply(Polynomial that) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		for (BigInteger pa : this.degrees) {
			for (BigInteger pb : that.degrees) {
				BigInteger sum = pa.add(pb);
				// xor the result
				if (dgrs.contains(sum))
					dgrs.remove(sum);
				else
					dgrs.add(sum);
			}
		}
		return new Polynomial(dgrs);
	}

	/**
	 * Computes (this & that) in GF(2^k)
	 */
	public Polynomial and(Polynomial that) {
		TreeSet<BigInteger> dgrs = this.createDegreesCollectionCopy();
		dgrs.retainAll(that.degrees);
		return new Polynomial(dgrs);
	}

	/**
	 * Computes (this | that) in GF(2^k)
	 */
	public Polynomial or(Polynomial that) {
		TreeSet<BigInteger> dgrs = this.createDegreesCollectionCopy();
		dgrs.addAll(that.degrees);
		return new Polynomial(dgrs);
	}

	/**
	 * Computes (this ^ that) in GF(2^k)
	 */
	public Polynomial xor(Polynomial that) {
		TreeSet<BigInteger> dgrs0 = this.createDegreesCollectionCopy();
		dgrs0.removeAll(that.degrees);
		TreeSet<BigInteger> dgrs1 = that.createDegreesCollectionCopy();
		dgrs1.removeAll(this.degrees);
		dgrs1.addAll(dgrs0);
		return new Polynomial(dgrs1);
	}

	/**
	 * Computes (this mod that) in GF(2^k) using synthetic division
	 */
	public Polynomial mod(Polynomial that) {
		BigInteger da = this.degree();
		BigInteger db = that.degree();
		Polynomial register = new Polynomial(this.degrees);
		for (BigInteger i = da.subtract(db); i.compareTo(BigInteger.ZERO) >= 0; i = i.subtract(BigInteger.ONE)) {
			if (register.hasDegree(i.add(db))) {
				Polynomial shifted = that.shiftLeft(i);
				register = register.xor(shifted);
			}
		}
		return register;
	}

	/**
	 * Computes (this << shift) in GF(2^k)
	 */
	public Polynomial shiftLeft(BigInteger shift) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		for (BigInteger degree : degrees) {
			BigInteger shifted = degree.add(shift);
			dgrs.add(shifted);
		}
		return new Polynomial(dgrs);
	}

	/**
	 * Computes (this >> shift) in GF(2^k)
	 */
	public Polynomial shiftRight(BigInteger shift) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		for (BigInteger degree : degrees) {
			BigInteger shifted = degree.subtract(shift);
			if (shifted.compareTo(BigInteger.ZERO) < 0)
				continue;
			dgrs.add(shifted);
		}
		return new Polynomial(dgrs);
	}

	/**
	 * Tests if there exists a term with degree k
	 */
	public boolean hasDegree(BigInteger k) {
		return degrees.contains(k);
	}

	/**
	 * Sets the coefficient of the term with degree k to 1
	 */
	public Polynomial setDegree(BigInteger k) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		dgrs.addAll(this.degrees);
		dgrs.add(k);
		return new Polynomial(dgrs);
	}

	/**
	 * Sets the coefficient of the term with degree k to 0
	 */
	public Polynomial clearDegree(BigInteger k) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		dgrs.addAll(this.degrees);
		dgrs.remove(k);
		return new Polynomial(dgrs);
	}

	/**
	 * Toggles the coefficient of the term with degree k
	 */
	public Polynomial toggleDegree(BigInteger k) {
		TreeSet<BigInteger> dgrs = createDegreesCollection();
		dgrs.addAll(this.degrees);
		if (dgrs.contains(k)) {
			dgrs.remove(k);
		} else {
			dgrs.add(k);
		}
		return new Polynomial(dgrs);
	}

	/**
	 * Computes (this^e mod m).
	 * 
	 * This algorithm requires at most this.degree() + m.degree() space.
	 * 
	 * http://en.wikipedia.org/wiki/Modular_exponentiation
	 */
	public Polynomial modPow(BigInteger e, Polynomial m) {
		Polynomial result = Polynomial.ONE;
		Polynomial b = new Polynomial(this);
		while (e.bitCount() != 0) {
			if (e.testBit(0)) {
				result = result.multiply(b).mod(m);
			}
			e = e.shiftRight(1);
			b = b.multiply(b).mod(m);
		}
		return result;
	}

	/**
	 * Computes the greatest common divisor between polynomials using Euclid's
	 * algorithm
	 * 
	 * http://en.wikipedia.org/wiki/Euclids_algorithm
	 */
	public Polynomial gcd(Polynomial that) {
		Polynomial a = new Polynomial(this);
		while (!that.isEmpty()) {
			Polynomial t = new Polynomial(that);
			that = a.mod(that);
			a = t;
		}
		return a;
	}

	/**
	 * Construct a BigInteger whose value represents this polynomial. This can
	 * lose information if the degrees of the terms are larger than
	 * Integer.MAX_VALUE;
	 */
	public BigInteger toBigInteger() {
		BigInteger b = BigInteger.ZERO;
		for (BigInteger degree : degrees) {
			b = b.setBit((int) degree.longValue());
		}
		return b;
	}
	
	/**
	 * Technically accurate but slow as hell.
	 */
	public BigInteger toBigIntegerAccurate() {
		BigInteger b = BigInteger.ZERO;
		for (BigInteger degree : degrees) {
			BigInteger term = BigInteger.ONE;
			for (BigInteger i = BigInteger.ONE; i.compareTo(degree) <= 0; i = i.add(BigInteger.ONE)) {
				term = term.shiftLeft(1);
			}
			b.add(term);
		}
		return b;
	}

	/**
	 * Returns a string of hex characters representing this polynomial
	 */
	public String toHexString() {
		return toBigInteger().toString(16).toUpperCase();
	}

	/**
	 * Returns a string of digits presenting this polynomial
	 */
	public String toDecimalString() {
		return toBigInteger().toString();
	}

	/**
	 * Returns a string of binary digits presenting this polynomial
	 */
	public String toBinaryString() {
		StringBuffer str = new StringBuffer();
		for (BigInteger deg = degree(); deg.compareTo(BigInteger.ZERO) >= 0; deg = deg
				.subtract(BigInteger.ONE)) {
			if (degrees.contains(deg)) {
				str.append("1");
			} else {
				str.append("0");
			}
		}
		return str.toString();
	}

	/**
	 * Returns standard ascii representation of this polynomial in the form:
	 * 
	 * e.g.: x^8 + x^4 + x^3 + x + 1
	 */
	public String toPolynomialString() {
		StringBuffer str = new StringBuffer();
		for (BigInteger degree : degrees) {
			if (str.length() != 0) {
				str.append(" + ");
			}
			if (degree.compareTo(BigInteger.ZERO) == 0) {
				str.append("1");
			} else {
				str.append("x^" + degree);
			}
		}
		return str.toString();
	}

	/**
	 * Default toString override uses the ascii representation
	 */
	@Override
	public String toString() {
		return toPolynomialString();
	}

	/**
	 * Tests the reducibility of the polynomial
	 */
	public boolean isReducible() {
		return getReducibility() == Reducibility.REDUCIBLE;
	}

	/**
	 * Tests the reducibility of the polynomial
	 */
	public Reducibility getReducibility() {
		// test trivial cases
		if (this.compareTo(Polynomial.ONE) == 0)
			return Reducibility.REDUCIBLE;
		if (this.compareTo(Polynomial.X) == 0)
			return Reducibility.REDUCIBLE;

		// do full-on reducibility test
		return getReducibilityBenOr();
	}

	/**
	 * BenOr Reducibility Test
	 * 
	 * Tests and Constructions of Irreducible Polynomials over Finite Fields
	 * (1997) Shuhong Gao, Daniel Panario
	 * 
	 * http://citeseer.ist.psu.edu/cache/papers/cs/27167/http:zSzzSzwww.math.clemson.eduzSzfacultyzSzGaozSzpaperszSzGP97a.pdf/gao97tests.pdf
	 */
	protected Reducibility getReducibilityBenOr() {
		final long degree = this.degree().longValue();
		for (int i = 1; i <= (int) (degree / 2); i++) {
			Polynomial b = reduceExponent(i);
			Polynomial g = this.gcd(b);
			if (g.compareTo(Polynomial.ONE) != 0)
				return Reducibility.REDUCIBLE;
		}

		return Reducibility.IRREDUCIBLE;
	}

	/**
	 * Rabin's Reducibility Test
	 * 
	 * This requires the distinct prime factors of the degree, so we don't use
	 * it. But this could be faster for prime degree polynomials
	 */
	protected Reducibility getReducibilityRabin(int[] factors) {
		int degree = (int) degree().longValue();
		for (int i = 0; i < factors.length; i++) {
			int n_i = factors[i];
			Polynomial b = reduceExponent(n_i);
			Polynomial g = this.gcd(b);
			if (g.compareTo(Polynomial.ONE) != 0)
				return Reducibility.REDUCIBLE;
		}

		Polynomial g = reduceExponent(degree);
		if (!g.isEmpty())
			return Reducibility.REDUCIBLE;

		return Reducibility.IRREDUCIBLE;
	}

	/**
	 * Computes ( x^(2^p) - x ) mod f
	 * 
	 * This function is useful for computing the reducibility of the polynomial
	 */
	private Polynomial reduceExponent(final int p) {
		// compute (x^q^p mod f)
		BigInteger q_to_p = Q.pow(p);
		Polynomial x_to_q_to_p = X.modPow(q_to_p, this);

		// subtract (x mod f)
		return x_to_q_to_p.xor(X).mod(this);
	}

	/**
	 * Compares this polynomial to the other
	 */
	public int compareTo(Polynomial o) {
		int cmp = degree().compareTo(o.degree());
		if (cmp != 0) return cmp;
		// get first degree difference
		Polynomial x = this.xor(o);
		if (x.isEmpty()) return 0;
		return this.hasDegree(x.degree()) ? 1 : -1;
	}
}
