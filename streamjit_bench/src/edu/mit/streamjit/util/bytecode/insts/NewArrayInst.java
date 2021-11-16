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
package edu.mit.streamjit.util.bytecode.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.ArrayType;

/**
 * Creates a primitive or reference array, initializing at least one of its
 * dimensions.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/16/2013
 */
public final class NewArrayInst extends Instruction {
	public NewArrayInst(ArrayType type, int dimensionsToCreate) {
		super(type, dimensionsToCreate);
		checkArgument(dimensionsToCreate >= 1);
		checkArgument(dimensionsToCreate <= type.getDimensions());
	}
	public NewArrayInst(ArrayType type, Value... dimensions) {
		super(type, dimensions.length);
		for (int i = 0; i < dimensions.length; ++i)
			setOperand(i, dimensions[i]);
	}

	@Override
	public ArrayType getType() {
		return (ArrayType)super.getType();
	}

	@Override
	public Instruction clone(Function<Value, Value> operandMap) {
		Value[] dimensions = new Value[getNumOperands()];
		for (int i = 0; i < getNumOperands(); ++i)
			dimensions[i] = operandMap.apply(getOperand(i));
		return new NewArrayInst(getType(), dimensions);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkArgument(v.getType().isSubtypeOf(v.getType().getTypeFactory().getType(int.class)));
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getName());
		sb.append(" (").append(getType()).append(") = new ").append(getType().getElementType());
		for (int i = 0; i < getType().getDimensions(); ++i) {
			sb.append('[');
			if (i < getNumOperands())
				sb.append(getOperand(i).getName());
			sb.append(']');
		}
		return sb.toString();
	}
}
