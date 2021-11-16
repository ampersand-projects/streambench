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
import edu.mit.streamjit.util.bytecode.Klass;

/**
 * An array type.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/7/2013
 */
public final class ArrayType extends ReferenceType {
	ArrayType(Klass klass) {
		super(klass);
		checkArgument(klass.isArray(), "not array: %s", klass);
	}

	public int getDimensions() {
		return getKlass().getDimensions();
	}

	public RegularType getComponentType() {
		return getTypeFactory().getRegularType(getKlass().getComponentKlass());
	}

	public RegularType getElementType() {
		return getTypeFactory().getRegularType(getKlass().getElementKlass());
	}

	@Override
	public boolean isSubtypeOf(Type other) {
		if (other instanceof ArrayType) {
			RegularType ct = getComponentType();
			RegularType oct = ((ArrayType)other).getComponentType();
			if (ct instanceof PrimitiveType && oct instanceof PrimitiveType)
				return ct.equals(oct);
			else if (ct instanceof ReferenceType && oct instanceof ReferenceType)
				return ct.isSubtypeOf(oct);
			else
				return false;
		} else if (other instanceof ReferenceType) {
			Klass rtk = ((ReferenceType)other).getKlass();
			//Object, Cloneable or Serializable
			return rtk.equals(getKlass().getSuperclass()) || getKlass().interfaces().contains(rtk);
		} else
			return false;
	}

	@Override
	public String getDescriptor() {
		return "["+getComponentType().getDescriptor();
	}
}
