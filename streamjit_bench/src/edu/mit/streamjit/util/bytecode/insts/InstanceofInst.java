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
import edu.mit.streamjit.util.bytecode.types.ReferenceType;

/**
 * Tests if an object is of a particular type.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/15/2013
 */
public final class InstanceofInst extends Instruction {
	private final ReferenceType testType;
	public InstanceofInst(ReferenceType testType) {
		super(testType.getTypeFactory().getPrimitiveType(boolean.class), 1);
		this.testType = testType;
	}
	public InstanceofInst(ReferenceType testType, Value v) {
		this(testType);
		setOperand(0, v);
	}

	public ReferenceType getTestType() {
		return testType;
	}

	@Override
	public InstanceofInst clone(Function<Value, Value> operandMap) {
		return new InstanceofInst(getTestType(), operandMap.apply(getOperand(0)));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 1);
		checkArgument(v.getType().isSubtypeOf(v.getType().getTypeFactory().getType(Object.class)));
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s (%s) = %s instanceof %s",
				getName(), getType(), getOperand(0).getName(), testType);
	}
}
