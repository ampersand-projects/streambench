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

import edu.mit.streamjit.util.bytecode.types.FieldType;
import edu.mit.streamjit.util.bytecode.types.RegularType;

/**
 * Represents a local variable allocated in a method's stack frame which can be
 * loaded or stored and is not subject to SSA form.  This maps directly to the
 * JVM's notion of local variables, except that LocalVariables do not change
 * type on a write like the JVM's local variables do.  LocalVariables are
 * analogous to LLVM's allocas-in-the-entry-block pattern.
 *
 * LocalVariables have field type; use the load and store instructions (without
 * an instance reference) to access their values.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 6/13/2013
 */
public class LocalVariable extends Value implements Parented<Method> {
	@ParentedList.Previous
	private LocalVariable previous;
	@ParentedList.Next
	private LocalVariable next;
	@ParentedList.Parent
	private Method parent;

	public LocalVariable(RegularType type, String name, Method parent) {
		super(type.getTypeFactory().getFieldType(type), name);
		if (parent != null)
			parent.localVariables().add(this);
	}

	@Override
	public FieldType getType() {
		return (FieldType)super.getType();
	}

	@Override
	public Method getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return String.format("%s %s",
				getType().getFieldType(),
				getName());
	}
}
