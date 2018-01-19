/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.collections;

import java.io.Externalizable;



import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.opendedup.hashing.HashFunctionPool;

public class QuickList<E> implements java.util.List<E>, Externalizable {

	private int size = 0;
	private int arraySize = 0;
	private transient E[] array;

	public static byte[] FREE = new byte[HashFunctionPool.hashLength];
	public static byte[] REMOVED = new byte[HashFunctionPool.hashLength];

	public QuickList(int size) {
		this.arraySize = size;
		array = this.newElementArray(this.arraySize);
	}

	public QuickList() {

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

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput arg0) throws IOException,
			ClassNotFoundException {
		this.arraySize = arg0.readInt();
		this.size = arg0.readInt();
		array = this.newElementArray(this.arraySize);
		for (int i = 0; i < this.size; i++) {
			this.array[i] = (E) arg0.readObject();
		}
	}

	@Override
	public void writeExternal(ObjectOutput arg0) throws IOException {
		arg0.writeInt(arraySize);
		arg0.writeInt(size);
		for (int i = 0; i < this.size; i++) {
			arg0.writeObject(this.array[i]);
		}
	}

	

}
