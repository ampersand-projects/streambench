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
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.Type;
import java.util.Iterator;

/**
 * A phi instruction resolves conflicting definitions from predecessor predecessors.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/15/2013
 */
public final class PhiInst extends Instruction {
	public PhiInst(Type type) {
		super(type);
	}

	public Value get(BasicBlock b) {
		int bbi = Iterables.indexOf(operands(), Predicates.<Value>equalTo(b));
		return bbi != -1 ? getOperand(bbi+1) : null;
	}

	public Value put(BasicBlock b, Value v) {
		checkNotNull(b);
		checkNotNull(v);
		checkArgument(v.getType().isSubtypeOf(getType()), "%s not a %s", v, getType());
		int bbi = Iterables.indexOf(operands(), Predicates.<Value>equalTo(b));
		Value oldVal = get(b);
		if (bbi != -1)
			setOperand(bbi+1, v);
		else {
			addOperand(getNumOperands(), b);
			addOperand(getNumOperands(), v);
		}
		return oldVal;
	}

	public FluentIterable<BasicBlock> predecessors() {
		return operands().filter(BasicBlock.class);
	}

	public FluentIterable<Value> incomingValues() {
		return operands().filter(Predicates.not(Predicates.instanceOf(BasicBlock.class)));
	}

	@Override
	public PhiInst clone(Function<Value, Value> operandMap) {
		PhiInst i = new PhiInst(getType());
		Iterator<BasicBlock> blocks = predecessors().iterator();
		Iterator<Value> values = incomingValues().iterator();
		while (blocks.hasNext())
			i.put((BasicBlock)operandMap.apply(blocks.next()), operandMap.apply(values.next()));
		return i;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getName());
		sb.append(" (").append(getType()).append(") = phi ");
		for (Iterator<Value> i = operands().iterator(); i.hasNext();)
			sb.append("[").append(i.next().getName()).append(", ").append(i.next().getName()).append("], ");
		sb.delete(sb.length()-2, sb.length());
		return sb.toString();
	}
}
