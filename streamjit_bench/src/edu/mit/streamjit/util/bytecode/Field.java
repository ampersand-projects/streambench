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

import com.google.common.base.Joiner;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.util.bytecode.types.FieldType;
import edu.mit.streamjit.util.bytecode.types.TypeFactory;
import java.util.EnumSet;
import java.util.Set;

/**
 * TODO: field static-ness is immutable even if the field isn't, because it's
 * part of the type and Values can't change type (would screw up loads and
 * stores, in this case)
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/3/2013
 */
public final class Field extends Value implements Accessible, Parented<Klass> {
	@IntrusiveList.Previous
	private Field previous;
	@IntrusiveList.Next
	private Field next;
	@ParentedList.Parent
	private Klass parent;
	private final Set<Modifier> modifiers;
	public Field(java.lang.reflect.Field f, Klass parent, Module module) {
		super(module.types().getFieldType(f), f.getName());
		//parent is set by our parent adding us to its list prior to making it
		//unmodifiable.  (We can't add ourselves and have the list wrapped
		//unmodifiable later because it's stored in a final field.)
		this.modifiers = Sets.immutableEnumSet(Modifier.fromFieldBits(Shorts.checkedCast(f.getModifiers())));
	}
	public Field(RegularType type, String name, Set<Modifier> modifiers, Klass parent) {
		super(typeHelper(type, parent, modifiers), name);
		checkNotNull(type);
		checkNotNull(name);
		checkNotNull(modifiers);
		this.modifiers = EnumSet.noneOf(Modifier.class);
		modifiers().addAll(modifiers);
		if (parent != null)
			parent.fields().add(this);
	}
	private static FieldType typeHelper(RegularType type, Klass parent, Set<Modifier> modifiers) {
		TypeFactory tf = parent.getParent().types();
		if (modifiers.contains(Modifier.STATIC))
			return tf.getFieldType(type);
		else
			return tf.getFieldType(tf.getReferenceType(parent), type);
	}

	public boolean isMutable() {
		return (parent == null) || parent.isMutable();
	}

	public java.lang.reflect.Field getBackingField() {
		//We don't call this very often (if at all), so look it up every time
		//rather than burn a field on all Fields.
		Class<?> klass = getParent().getBackingClass();
		try {
			return klass != null ? klass.getDeclaredField(getName()) : null;
		} catch (NoSuchFieldException ex) {
			throw new AssertionError(String.format("Can't happen! Class %s doesn't have a %s field?", klass, getName()), ex);
		}
	}

	@Override
	public void setName(String name) {
		checkArgument(isMutable());
		super.setName(name);
	}
	@Override
	public FieldType getType() {
		return (FieldType)super.getType();
	}
	public Set<Modifier> modifiers() {
		return modifiers;
	}

	@Override
	public Access getAccess() {
		return Access.fromModifiers(modifiers());
	}

	@Override
	public void setAccess(Access access) {
		modifiers().removeAll(Access.allAccessModifiers());
		modifiers().addAll(access.modifiers());
	}

	public boolean isStatic() {
		return modifiers().contains(Modifier.STATIC);
	}

	@Override
	public Klass getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return String.format("%s %s %s",
				Joiner.on(' ').join(modifiers()),
				getType().getFieldType(),
				getName());
	}
}
