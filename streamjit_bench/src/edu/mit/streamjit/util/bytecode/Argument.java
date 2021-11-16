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

import edu.mit.streamjit.util.bytecode.types.RegularType;

/**
 * An Argument represents an argument to a Method.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/6/2013
 */
public class Argument extends Value implements Parented<Method> {
	private final Method parent;
	public Argument(Method parent, RegularType type) {
		super(type);
		this.parent = parent;
	}
	public Argument(Method parent, RegularType type, String name) {
		super(type, name);
		this.parent = parent;
	}

	public boolean isReceiver() {
		return parent.hasReceiver() && parent.arguments().indexOf(this) == 0;
	}

	@Override
	public Method getParent() {
		return parent;
	}

	@Override
	public RegularType getType() {
		return (RegularType)super.getType();
	}

	@Override
	public String toString() {
		if (getName() != null)
			return getName();
		return getClass().getSimpleName()+"@"+hashCode();
	}
}
