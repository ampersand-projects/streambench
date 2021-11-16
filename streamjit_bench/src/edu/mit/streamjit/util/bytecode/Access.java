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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;

/**
 * Represents an access kind.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/3/2013
 */
public enum Access {
	PUBLIC(Modifier.PUBLIC), PROTECTED(Modifier.PROTECTED),
	PACKAGE_PRIVATE(), PRIVATE(Modifier.PRIVATE);
	private static final ImmutableSet<Modifier> ACCESS_MODIFIERS =
			Sets.immutableEnumSet(Modifier.PUBLIC, Modifier.PROTECTED, Modifier.PRIVATE);
	private final ImmutableSet<Modifier> modifiers;
	private Access(Modifier... modifiers) {
		this.modifiers = Sets.immutableEnumSet(Arrays.asList(modifiers));
	}

	public ImmutableSet<Modifier> modifiers() {
		return modifiers;
	}

	public static Access fromModifiers(Set<Modifier> modifiers) {
		Set<Modifier> active = Sets.intersection(allAccessModifiers(), modifiers);
		for (Access a : values())
			if (active.equals(a.modifiers()))
				return a;
		throw new IllegalArgumentException("bad access modifiers: "+active);
	}

	public static ImmutableSet<Modifier> allAccessModifiers() {
		return ACCESS_MODIFIERS;
	}
}
