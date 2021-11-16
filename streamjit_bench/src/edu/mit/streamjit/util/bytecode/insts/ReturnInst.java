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
import edu.mit.streamjit.util.bytecode.types.ReturnType;
import edu.mit.streamjit.util.bytecode.types.VoidType;

/**
 * Returns to the caller's stack frame, possibly with a value.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/11/2013
 */
public final class ReturnInst extends TerminatorInst {
	private final ReturnType returnType;
	public ReturnInst(ReturnType returnType) {
		super(returnType.getTypeFactory(), returnType instanceof VoidType ? 0 : 1);
		this.returnType = returnType;
	}
	public ReturnInst(ReturnType returnType, Value returnValue) {
		super(returnType.getTypeFactory(), returnType instanceof VoidType ? 0 : 1);
		this.returnType = returnType;
		if (returnType instanceof VoidType)
			checkArgument(returnValue == null, "returning a value with void ReturnInst: %s", returnValue);
		setOperand(0, returnValue);
	}

	public ReturnType getReturnType() {
		return returnType;
	}

	@Override
	public ReturnInst clone(Function<Value, Value> operandMap) {
		if (getNumOperands() == 1)
			return new ReturnInst(getReturnType(), operandMap.apply(getOperand(0)));
		return new ReturnInst(getReturnType());
	}

	@Override
	protected void checkOperand(int i, Value v) {
		if (getReturnType() instanceof VoidType)
			throw new IllegalStateException("void returns don't take operands");
		checkArgument(i == 0, "ReturnInsts take only one operand: %s %s", i, v);
		checkArgument(v.getType().isSubtypeOf(returnType), "not subtype of %s: %s", returnType, v);
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s: return %s", getName(),
				getReturnType() instanceof VoidType ? "void" : getOperand(0).getName());
	}
}
