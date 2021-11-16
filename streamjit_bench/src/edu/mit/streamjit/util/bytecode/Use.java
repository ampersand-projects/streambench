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
package edu.mit.streamjit.util.bytecode;

import java.util.Objects;

/**
 * A Use represents a single use of a Value.  Note that a User can use the same
 * value more than once if it has multiple operands; this class thus keeps an
 * operand index to disambiguate.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/6/2013
 */
public class Use {
	private final User user;
	private int operandIndex;
	private Value value;

	public Use(User user, int operandIndex, Value value) {
		this.user = user;
		this.operandIndex = operandIndex;
		this.value = value;
		if (value != null)
			value.addUse(this);
	}

	public User getUser() {
		return user;
	}

	public int getOperandIndex() {
		return operandIndex;
	}

	//for internal use only!
	void setOperandIndex(int index) {
//		assert ReflectionUtils.calledDirectlyFrom(User.class);
		operandIndex = index;
	}

	public Value getOperand() {
		return value;
	}

	public void setOperand(Value other) {
		user.checkOperandInternal(operandIndex, other);
		if (Objects.equals(getOperand(), other))
			return;
		if (value != null)
			value.removeUse(this);
		this.value = other;
		if (other != null)
			other.addUse(this);
	}

	@Override
	public String toString() {
		return "Use{" + "user=" + user + ", operandIndex=" + operandIndex + '}';
	}
}
