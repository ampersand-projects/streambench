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
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.Type;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/15/2013
 */
public final class CastInst extends Instruction {
	public CastInst(Type targetType) {
		super(targetType, 1);
	}
	public CastInst(Type targetType, Value source) {
		this(targetType);
		setOperand(0, source);
	}

	@Override
	public CastInst clone(Function<Value, Value> operandMap) {
		return new CastInst(getType(), operandMap.apply(getOperand(0)));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		//TODO: check the cast is legal/possible
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s (%s) = cast %s to %s",
				getName(), getType(), getOperand(0).getName(), getType());
	}
}
