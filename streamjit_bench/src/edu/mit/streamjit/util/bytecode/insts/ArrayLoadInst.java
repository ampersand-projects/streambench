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
 * Stores a value in an array.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/16/2013
 */
public final class ArrayLoadInst extends Instruction {
	public ArrayLoadInst(Value array, Value index) {
		super(((ArrayType)array.getType()).getComponentType(), 2);
		setOperand(0, array);
		setOperand(1, index);
	}

	public Value getArray() {
		return getOperand(0);
	}
	public void setArray(Value v) {
		setOperand(0, v);
	}
	public Value getIndex() {
		return getOperand(1);
	}
	public void setIndex(Value v) {
		setOperand(1, v);
	}

	@Override
	public ArrayLoadInst clone(Function<Value, Value> operandMap) {
		return new ArrayLoadInst(operandMap.apply(getArray()), operandMap.apply(getIndex()));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		if (i == 0) {
			checkArgument(v.getType() instanceof ArrayType);
			checkArgument(((ArrayType)v.getType()).getComponentType().isSubtypeOf(getType()));
		} else if (i == 1)
			checkArgument(v.getType().isSubtypeOf(getType().getTypeFactory().getType(int.class)));
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s (%s) = arrayload %s [%s]",
				getName(), getType(), getOperand(0).getName(), getOperand(1).getName());
	}
}
