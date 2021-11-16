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

import static com.google.common.base.Preconditions.*;
import java.io.PrintWriter;
import java.util.Arrays;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;

/**
 * Builds a .class file (as a byte[]) from a Klass.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 4/17/2013
 */
public final class KlassUnresolver {
	public static byte[] unresolve(Klass k) {
		checkNotNull(k);
		//TODO: permit this for testing
		//checkArgument(k.isMutable());
		return new KlassUnresolver(k).unresolve();
	}

	private final Klass klass;
	private final ClassNode classNode;
	private KlassUnresolver(Klass k) {
		this.klass = k;
		this.classNode = new ClassNode(Opcodes.ASM4);
	}

	private byte[] unresolve() {
		this.classNode.version = Opcodes.V1_7;
		this.classNode.access = Modifier.toBits(klass.modifiers());
		this.classNode.name = internalName(klass);
		assert klass.getSuperclass() != null || Object.class.equals(klass.getBackingClass()) : klass;
		this.classNode.superName = internalName(klass.getSuperclass());
		for (Klass k : klass.interfaces())
			this.classNode.interfaces.add(internalName(k));

		for (Field f : klass.fields()) {
			FieldNode fn = new FieldNode(Opcodes.ASM4,
					Modifier.toBits(f.modifiers()),
					f.getName(),
					f.getType().getFieldType().getDescriptor(),
					null, null);
			this.classNode.fields.add(fn);
		}

		for (Method m : klass.methods())
			this.classNode.methods.add(MethodUnresolver.unresolve(m));

		ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
		ClassVisitor cv = cw;
//		boolean assertionsEnabled = false;
//		assert assertionsEnabled = true; //intentional side effect
//		if (assertionsEnabled)
//			cv = new CheckClassAdapter(cv, true);
		classNode.accept(cv);
		return cw.toByteArray();
	}

	private String internalName(Klass k) {
		return k.getName().replace('.', '/');
	}

	public static void main(String[] args) {
		Module m = new Module();
		Klass k = m.getKlass(Module.class);
		for (Method method : k.methods())
			if (method.isResolvable()) {
				method.resolve();
				method.dump(new PrintWriter(System.out, true));
			}
		byte[] b = unresolve(k);
		System.out.println(Arrays.toString(b));
		System.out.println(b.length);
	}
}
