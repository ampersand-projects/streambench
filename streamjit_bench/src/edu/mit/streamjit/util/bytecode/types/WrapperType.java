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

import static com.google.common.base.Preconditions.*;
import com.google.common.primitives.Primitives;
import edu.mit.streamjit.util.bytecode.Klass;

/**
 * A wrapper type.  (Note that java.lang.Void is not considered a wrapper type
 * because void is not considered a primitive type.)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/9/2013
 */
public final class WrapperType extends ReferenceType {
	WrapperType(Klass klass) {
		super(klass);
		Class<?> backer = klass.getBackingClass();
		checkArgument(backer != null && Primitives.isWrapperType(backer) && !backer.equals(Void.class),
				"not a wrapper type: %s", klass);
	}

	public PrimitiveType unwrap() {
		//Wrapper types are always backed by Classes.
		return getTypeFactory().getPrimitiveType(getModule().getKlass(Primitives.unwrap(getKlass().getBackingClass())));
	}
}
