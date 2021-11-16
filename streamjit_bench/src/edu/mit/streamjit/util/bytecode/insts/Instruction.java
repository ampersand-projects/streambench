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

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Function;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Parented;
import edu.mit.streamjit.util.bytecode.ParentedList;
import edu.mit.streamjit.util.bytecode.User;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.Type;
import edu.mit.streamjit.util.bytecode.IntrusiveList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/11/2013
 */
public abstract class Instruction extends User implements Parented<BasicBlock> {
	@IntrusiveList.Previous
	private Instruction previous;
	@IntrusiveList.Next
	private Instruction next;
	@ParentedList.Parent
	private BasicBlock parent;

	protected Instruction(Type type) {
		super(type);
	}

	protected Instruction(Type type, int operands) {
		super(type, operands);
	}

	protected Instruction(Type type, String name) {
		super(type, name);
	}

	protected Instruction(Type type, int operands, String name) {
		super(type, operands, name);
	}

	@Override
	public BasicBlock getParent() {
		return parent;
	}

	/**
	 * Removes this instruction from its parent basic block.  This instruction
	 * is not otherwise modified.
	 *
	 * Typically, this method is used when moving an instruction from one block
	 * to another; this method returns this instruction, to permit {@code
	 * block.instructions().add(inst.removeFromParent())}.  If removing an
	 * instruction permanently, use {@link #eraseFromParent()}.
	 * @return this instruction, for chaining
	 */
	public Instruction removeFromParent() {
		checkState(getParent() != null);
		getParent().instructions().remove(this);
		return this;
	}

	/**
	 * Removes this instruction from its parent basic block and drops its
	 * operands.  This method is intended for use when the instruction is being
	 * removed permanently; if moving an instruction from one block to another,
	 * use {@link #removeFromParent()}.
	 */
	public void eraseFromParent() {
		removeFromParent();
		dropAllOperands();
	}

	/**
	 * Replaces this instruction with the given instruction, by adding it to
	 * this instruction's parent basic block just prior to this instruction,
	 * replacing all uses of this instruction with the given instruction,
	 * removing this instruction from its parent, and dropping all of this
	 * instruction's operands.
	 * @param replacement the instruction to replace this instruction with
	 */
	public void replaceInstWithInst(Instruction replacement) {
		//If we're using replaceInstWithInst, we expect the replacement to end
		//up in a basic block, which is only possible if we're in one already.
		checkState(getParent() != null);
		checkArgument(replacement.getParent() == null);

		getParent().instructions().add(getParent().instructions().indexOf(this), replacement);
		replaceAllUsesWith(replacement);
		getParent().instructions().remove(this);
		dropAllOperands();
	}

	/**
	 * Replaces this instruction with the given instructions similarly to
	 * {@link #replaceInstWithInst(Instruction)}, using a specific value to
	 * replace uses, but adding all the given instructions in order just prior
	 * to this instruction.
	 * @param replacement the value to replace this instruction's uses with
	 * @param insts the instructions to replace this instruction with
	 */
	public void replaceInstWithInsts(Value replacement, List<Instruction> insts) {
		checkState(getParent() != null);
		for (Instruction i : insts)
			checkArgument(i.getParent() == null);

		//Get a sublist, then addAll() at the end of it.
		getParent().instructions().subList(0, getParent().instructions().indexOf(this)).addAll(insts);
		replaceAllUsesWith(replacement);
		getParent().instructions().remove(this);
		dropAllOperands();
	}

	/**
	 * Replaces this instruction with the given instructions similarly to
	 * {@link #replaceInstWithInst(Instruction)}, using a specific value to
	 * replace uses, but adding all the given instructions in order just prior
	 * to this instruction.
	 * @param replacement the value to replace this instruction's uses with
	 * @param insts the instructions to replace this instruction with
	 */
	public void replaceInstWithInsts(Value replacement, Instruction... insts) {
		replaceInstWithInsts(replacement, Arrays.asList(insts));
	}

	/**
	 * Replaces this instruction with the given value, by replacing all uses of
	 * this instruction with the given value, removing this instruction from its
	 * parent, and dropping all of this instruction's operands.
	 *
	 * The given value is not modified in any way except by the possible
	 * addition of extra uses (when this instruction's uses are replaced).  In
	 * particular, if the given value is an instruction, it is not moved from
	 * wherever it is into this instruction's place.
	 * @param replacement the value to replace this instruction with
	 */
	public void replaceInstWithValue(Value replacement) {
		checkState(getParent() != null);

		replaceAllUsesWith(replacement);
		getParent().instructions().remove(this);
		dropAllOperands();
	}

	/**
	 * Clones this instruction, using the given function to map this
	 * instruction's operands to new operands.
	 * @param operandMap a function mapping values to values
	 * @return a clone of this instruction
	 */
	public abstract Instruction clone(Function<Value, Value> operandMap);
}
