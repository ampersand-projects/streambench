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
import edu.mit.streamjit.util.bytecode.Field;
import edu.mit.streamjit.util.bytecode.LocalVariable;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.FieldType;
import edu.mit.streamjit.util.bytecode.types.InstanceFieldType;

/**
 * Loads a static or instance field.  (Does not load array elements.)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/14/2013
 */
public final class LoadInst extends Instruction {
	public LoadInst(Field f) {
		super(checkNotNull(f).getType().getFieldType(), f.isStatic() ? 1 : 2);
		setOperand(0, f);
	}
	public LoadInst(Field f, Value v) {
		this(f);
		checkArgument(!f.isStatic(), "loading from static field %s with instance %s", f, v);
		setOperand(1, v);
	}
	public LoadInst(LocalVariable v) {
		super(checkNotNull(v).getType().getFieldType(), 1);
		setOperand(0, v);
	}

	public Value getLocation() {
		return getOperand(0);
	}
	public void setLocation(Value f) {
		setOperand(0, f);
	}
	public Value getInstance() {
		return getOperand(1);
	}
	public void setInstance(Value v) {
		setOperand(1, v);
	}

	@Override
	public Instruction clone(Function<Value, Value> operandMap) {
		Value newLocation = operandMap.apply(getLocation());
		if (getNumOperands() == 1)
			if (newLocation instanceof Field)
				return new LoadInst((Field)newLocation);
			else
				return new LoadInst((LocalVariable)newLocation);
		else
			return new LoadInst((Field)newLocation, operandMap.apply(getInstance()));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 2);
		if (i == 0) {
			checkArgument(v.getType() instanceof FieldType);
			FieldType ft = (FieldType)v.getType();
			checkArgument(ft.getFieldType().isSubtypeOf(getType()));
		} else if (i == 1) {
			Field f = (Field)getLocation();
			if (f != null) {
				checkState(f.getType() instanceof InstanceFieldType);
				checkArgument(v.getType().isSubtypeOf(((InstanceFieldType)f.getType()).getInstanceType()));
			}
		}
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		if (getLocation() instanceof LocalVariable) {
			return String.format("%s (%s) = load %s",
						getName(), getType(),
						getLocation().getName());
		} else {
			Field f = (Field)getLocation();
			if (f.isStatic())
				return String.format("%s (%s) = getstatic %s#%s",
						getName(), getType(),
						f.getParent().getName(), f.getName());
			else
				return String.format("%s (%s) = getfield %s#%s from %s",
						getName(), getType(),
						f.getParent().getName(), f.getName(),
						getOperand(1).getName());
		}
	}
}
