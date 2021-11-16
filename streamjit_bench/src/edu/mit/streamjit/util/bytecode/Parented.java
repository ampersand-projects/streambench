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
package edu.mit.streamjit.util.bytecode;

/**
 * Implementations of this interface can be logically associated with a parent
 * object.  Parented objects have at most one parent, but may have no parent.
 * The parent may be fixed for the life of the object, or may change.
 *
 * This interface deliberately does not provide a method for setting the parent.
 * How the parent is set is up to the implementor of the parent and parented
 * classes.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/12/2013
 */
public interface Parented<P> {
	/**
	 * Returns this object's parent, or null if this object has no parent.
	 * @return this object's parent, or null if this object has no parent
	 */
	public P getParent();
}
