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
package edu.mit.streamjit.util.bytecode.types;

import edu.mit.streamjit.util.bytecode.Module;

/**
 * The type of a BasicBlock.  Currently, this is a singleton type.  (We might
 * choose to make it per-Method at some point to prevent branch instructions
 * from branching to blocks in other methods, but as a Value's type is
 * immutable, that would mean BasicBlocks would be permanently attached to
 * Functions.  For now we'll preserve the flexibility to transplant BasicBlocks
 * or create free-floating BasicBlocks before inserting them into a Method.)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/6/2013
 */
public final class BasicBlockType extends Type {
	private final Module module;
	BasicBlockType(Module module) {
		this.module = module;
	}

	@Override
	public String toString() {
		return "BasicBlock";
	}

	@Override
	public int getCategory() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Module getModule() {
		return module;
	}

	@Override
	public TypeFactory getTypeFactory() {
		return getModule().types();
	}
}
