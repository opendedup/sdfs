package org.opendedup.collections;

import java.util.Collection;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.opendedup.hashing.HashFunctionPool;

public class QuickList<E> implements java.util.List<E> {

	private int size = 0;
	private int arraySize = 0;
	private transient E[] array;

	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];

	public QuickList(int size) {
		this.arraySize = size;
		array = this.newElementArray(this.arraySize);
	}

	@SuppressWarnings("unchecked")
	private E[] newElementArray(int size) {
		return (E[]) new Object[size];
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return this.size;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return (size == 0);
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E[] toArray() {
		// TODO Auto-generated method stub
		return this.array;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean add(E e) {
		this.array[size] = e;
		size++;
		return true;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		this.size = 0;

	}

	@Override
	public E get(int index) {
		return this.array[index];
	}

	@Override
	public E set(int location, E object) {
		E result = this.array[location];
		this.array[location] = object;
		this.setSize(location);
		return result;
	}

	private void setSize(int pos) {
		int pSz = pos + 1;
		if (pSz > size) {
			this.size = pSz;
		}
	}

	@Override
	public void add(int index, E element) {
		this.array[index] = element;
		this.setSize(index);

	}

	@Override
	public E remove(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ListIterator<E> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

}
