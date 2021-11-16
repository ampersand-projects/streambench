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
import com.google.common.collect.FluentIterable;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.TypeFactory;
import edu.mit.streamjit.util.bytecode.types.VoidType;

/**
 * A TerminatorInst is an instruction that produces control flow; that is, an
 * instruction that terminates a basic block.  TerminatorInsts never produce
 * values (they have void type).  (Note that LLVM's invoke instruction produces
 * a value when it completes normally, but we don't model exceptions from calls
 * as explicit control transfers.)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/11/2013
 */
public abstract class TerminatorInst extends Instruction {
	protected TerminatorInst(TypeFactory typeFactory) {
		super(typeFactory.getVoidType());
	}
	protected TerminatorInst(TypeFactory typeFactory, int operands) {
		super(typeFactory.getVoidType(), operands);
	}
	protected TerminatorInst(TypeFactory typeFactory, String name) {
		super(typeFactory.getVoidType(), name);
	}
	protected TerminatorInst(TypeFactory typeFactory, int operands, String name) {
		super(typeFactory.getVoidType(), operands, name);
	}

	@Override
	public VoidType getType() {
		return (VoidType)super.getType();
	}

	@Override
	public abstract TerminatorInst clone(Function<Value, Value> operandMap);

	/**
	 * Returns all this TerminatorInst's successors.  This is a filtered view of
	 * this instruction's BasicBlock operands, in the same order as they appear
	 * in the operand list (but not necessarily at the same index).  Note that
	 * a TerminatorInst may have no successors (if it's a ThrowInst).
	 * @return this TerminatorInst's successors
	 */
	public FluentIterable<BasicBlock> successors() {
		return operands().filter(BasicBlock.class);
	}
}
