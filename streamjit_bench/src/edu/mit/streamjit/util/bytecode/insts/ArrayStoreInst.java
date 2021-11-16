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
import edu.mit.streamjit.util.bytecode.types.NullType;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.util.bytecode.types.Type;

/**
 * Stores a value in an array.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/16/2013
 */
public final class ArrayStoreInst extends Instruction {
	public ArrayStoreInst(Value array, Value index, Value data) {
		super(array.getType().getTypeFactory().getVoidType(), 3);
		setOperand(0, array);
		setOperand(1, index);
		setOperand(2, data);
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
	public Value getData() {
		return getOperand(2);
	}
	public void setData(Value v) {
		setOperand(2, v);
	}

	@Override
	public ArrayStoreInst clone(Function<Value, Value> operandMap) {
		return new ArrayStoreInst(operandMap.apply(getArray()), operandMap.apply(getIndex()), operandMap.apply(getData()));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		final Type intType = getType().getTypeFactory().getType(int.class);
		//Check the new argument separately.
		if (i == 0)
			checkArgument(v.getType() instanceof ArrayType);
		else if (i == 1)
			checkArgument(v.getType().isSubtypeOf(intType));
		else if (i == 2)
			checkArgument(v.getType() instanceof RegularType || v.getType() instanceof NullType);
		//Consistency checks.
		Value array = i == 0 ? v : getArray();
		Value data = i == 2 ? v : getData();
		if (array != null && data != null) {
			//At the JVM level, all smaller-than-int types are just ints and can
			//be stored in any array of smaller-than-int.
			Type comp = ((ArrayType)array.getType()).getComponentType();
			if (comp.isSubtypeOf(intType))
				comp = intType;
			Type dt = data.getType();
			if (dt.isSubtypeOf(intType))
				dt = intType;
			checkArgument(dt.isSubtypeOf(comp));
		}
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s: arraystore %s [%s] = %s",
				getName(), getOperand(0).getName(), getOperand(1).getName(), getOperand(2).getName());
	}
}
