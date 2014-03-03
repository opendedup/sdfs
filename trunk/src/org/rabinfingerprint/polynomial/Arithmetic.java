package org.rabinfingerprint.polynomial;

public interface Arithmetic<T> {
	public T add(T that);
	public T subtract(T that);
	public T multiply(T that);
	public T and(T that);
	public T or(T that);
	public T xor(T that);
	public T mod(T that);
	public T gcd(T that);
}