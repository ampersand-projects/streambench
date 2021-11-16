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
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.ReferenceType;
import edu.mit.streamjit.util.bytecode.types.Type;

/**
 * A conditional branch.  Compares two values against one another and branches
 * to one of two basic blocks depending on the result.
 *
 * If the values are of primitive type, they must either both be subtypes of int
 * or be equal (long, float or double).  All comparison senses are available.
 *
 * If the values are of reference type, they need not have any particular type
 * relationship, but only the equals and not-equals senses are available.
 *
 * This class may represent a cmp-type JVM opcode in addition to a branch, and
 * comparisons against constants may map to special opcodes.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/13/2013
 */
public final class BranchInst extends TerminatorInst {
	public static enum Sense {EQ, NE, LT, GT, LE, GE};
	private final Sense sense;
	public BranchInst(Value a, Sense s, Value b, BasicBlock t, BasicBlock f) {
		super(a.getType().getTypeFactory(), 4);
		Type intType = a.getType().getTypeFactory().getPrimitiveType(int.class);
		Type objectType = a.getType().getTypeFactory().getReferenceType(Object.class);
		//This used to check instanceof ReferenceType, but NullType is okay too.
		checkArgument((a.getType().isSubtypeOf(objectType) && b.getType().isSubtypeOf(objectType)) ||
				(a.getType().isSubtypeOf(intType) && b.getType().isSubtypeOf(intType)) ||
				(a.getType().equals(b.getType())),
				"bad types for branch: %s %s", a, b);
		if (a.getType() instanceof ReferenceType)
			checkArgument(s == Sense.EQ || s == Sense.NE, "bad sense for ref branch: %s", s);
		setOperand(0, a);
		setOperand(1, b);
		setOperand(2, t);
		setOperand(3, f);
		this.sense = s;
	}

	public Sense getSense() {
		return sense;
	}

	@Override
	public BranchInst clone(Function<Value, Value> operandMap) {
		return new BranchInst(operandMap.apply(getOperand(0)), sense, operandMap.apply(getOperand(1)),
				(BasicBlock)operandMap.apply(getOperand(2)), (BasicBlock)operandMap.apply(getOperand(3)));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		//TODO: check type combo is legal, check BBs are really BBs
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s: branch if %s %s %s then %s else %s",
				getName(), getOperand(0).getName(), sense, getOperand(1).getName(),
				getOperand(2).getName(), getOperand(3).getName());
	}
}
