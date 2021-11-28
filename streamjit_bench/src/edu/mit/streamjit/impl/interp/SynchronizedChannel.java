/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.interp;

import java.util.Iterator;

/**
 * A Channel implementation that delegates to another Channel implementation,
 * using a lock to synchronize all its methods.  Note that this class' iterator
 * is not itself synchronized (and may or may not throw
 * ConcurrentModificationException depending on the underlying implementation).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/8/2013
 */
public class SynchronizedChannel<E> implements Channel<E>{
	private final Channel<E> delegate;
	public SynchronizedChannel(Channel<E> delegate) {
		if (delegate == null)
			throw new NullPointerException();
		this.delegate = delegate;
	}

	@Override
	public synchronized void push(E element) {
		delegate.push(element);
	}

	@Override
	public synchronized E peek(int index) {
		return delegate.peek(index);
	}

	@Override
	public synchronized E pop() {
		return delegate.pop();
	}

	@Override
	public synchronized int size() {
		return delegate.size();
	}

	@Override
	public synchronized boolean isEmpty() {
		return delegate.isEmpty();
	}

	@Override
	public synchronized Iterator<E> iterator() {
		return delegate.iterator();
	}

	@Override
	public synchronized String toString() {
		return delegate.toString();
	}
}
