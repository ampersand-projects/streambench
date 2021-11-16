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
package edu.mit.streamjit.util.bytecode.types;

import edu.mit.streamjit.util.bytecode.Module;

/**
 * The types of Values.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/6/2013
 */
public abstract class Type {
	Type() {}

	public abstract Module getModule();
	public abstract TypeFactory getTypeFactory();

	/**
	 * Tests if this Type is a subtype of the given Type.
	 *
	 * The subtype relation is reflexive and transitive, but not symmetric.
	 *
	 * By default, a Type is a subtype of another iff they are the same type,
	 * but subclasses can override this.
	 * @param other the type to compare against (must not be null)
	 * @return true iff this type is a subtype of the other type
	 */
	public boolean isSubtypeOf(Type other) {
		return equals(other);
	}

	/**
	 * Returns the category of this type, the number of local variables required
	 * to store it.
	 *
	 * All regular types are category 1 types except long and double, which are
	 * category 2 types.  The null type is a category 1 type.
	 *
	 * Other types do not have a category and will throw
	 * UnsupportedOperationException.
	 * @return this type's category
	 */
	public abstract int getCategory();
}
