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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.util.bytecode.Module;
import java.util.List;

/**
 * MethodType represents the type of a method, including the types of its
 * parameters (which must be RegularTypes) and its return type (which must be a
 * ReturnType).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/6/2013
 */
public class MethodType extends Type {
	private final ReturnType returnType;
	private final ImmutableList<RegularType> parameterTypes;
	MethodType(ReturnType returnType, List<RegularType> parameterTypes) {
		this.returnType = checkNotNull(returnType);
		this.parameterTypes = ImmutableList.copyOf(parameterTypes);
	}

	public ReturnType getReturnType() {
		return returnType;
	}

	public ImmutableList<RegularType> getParameterTypes() {
		return parameterTypes;
	}

	public MethodType withReturnType(ReturnType newReturnType) {
		return getTypeFactory().getMethodType(newReturnType, parameterTypes);
	}

	public MethodType prependArgument(RegularType newParameterType) {
		return getTypeFactory().getMethodType(returnType,
				ImmutableList.<RegularType>builder().add(newParameterType).addAll(parameterTypes).build());
	}

	public MethodType dropFirstArgument() {
		return getTypeFactory().getMethodType(returnType, parameterTypes.subList(1, parameterTypes.size()));
	}

	public MethodType appendArgument(RegularType newParameterType) {
		return getTypeFactory().getMethodType(returnType,
				ImmutableList.<RegularType>builder().addAll(parameterTypes).add(newParameterType).build());
	}

	public MethodType dropLastArgument() {
		return getTypeFactory().getMethodType(returnType, parameterTypes.subList(0, parameterTypes.size()-1));
	}

	public String getDescriptor() {
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		for (RegularType t : getParameterTypes())
			sb.append(t.getDescriptor());
		sb.append(')');
		sb.append(returnType.getDescriptor());
		return sb.toString();
	}

	@Override
	public int getCategory() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return returnType.toString() + '(' + Joiner.on(", ").join(parameterTypes) + ')';
	}

	@Override
	public Module getModule() {
		return returnType.getModule();
	}

	@Override
	public TypeFactory getTypeFactory() {
		return returnType.getTypeFactory();
	}
}
