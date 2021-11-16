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

import static com.google.common.base.Preconditions.checkArgument;
import edu.mit.streamjit.util.bytecode.Klass;

/**
 * A reference type, including array types.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/7/2013
 */
public class ReferenceType extends RegularType {
	public ReferenceType(Klass klass) {
		super(klass);
		//A RegularType that isn't a primitive type.  Primitives all have
		//backing classes, so we can check isPrimitive(); RegularType will
		//exclude void (for which isPrimitive() returns true).
		Class<?> backing = klass.getBackingClass();
		checkArgument(backing == null || !backing.isPrimitive(),
				"not a ReferenceType: %s", klass);
	}

	/**
	 * Returns true if this type is the given type, or this type's superclass
	 * is a subtype of the given type, or one of this type's superinterfaces is
	 * a subtype of the given type.  (Note that this definition is recursive.)
	 *
	 * This definition takes care of comparing ReferenceTypes to ArrayTypes; the
	 * other direction is handled by ArrayType.isSubtypeOf.
	 * @param other
	 * @return
	 */
	@Override
	public boolean isSubtypeOf(Type other) {
		if (equals(other))
			return true;
		Klass superclass = getKlass().getSuperclass();
		if (superclass != null && getTypeFactory().getType(superclass).isSubtypeOf(other))
			return true;
		for (Klass k : getKlass().interfaces())
			if (getTypeFactory().getType(k).isSubtypeOf(other))
				return true;
		return false;
	}

	@Override
	public String getDescriptor() {
		//We replace . with / because the JVM does for historical reasons.
		//(This format is called the "internal name".)
		return "L"+getKlass().getName().replace('.', '/')+";";
	}
}
