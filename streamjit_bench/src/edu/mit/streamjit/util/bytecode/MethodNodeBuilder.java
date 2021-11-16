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

import edu.mit.streamjit.util.bytecode.types.MethodType;
import java.io.IOException;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.MethodNode;

/**
 * Builds a MethodNode for the method with the given name and descriptor.
 */
public final class MethodNodeBuilder {
	private MethodNodeBuilder() {}
	public static MethodNode buildMethodNode(Class<?> klass, String methodName, String methodDescriptor) throws IOException, NoSuchMethodException {
		ClassReader r = new ClassReader(klass.getName());
		MethodNodeBuildingClassVisitor mnbcv = new MethodNodeBuildingClassVisitor(methodName, methodDescriptor);
		r.accept(mnbcv, ClassReader.EXPAND_FRAMES);
		MethodNode methodNode = mnbcv.getMethodNode();
		if (methodNode == null)
			throw new NoSuchMethodException(klass.getName() + "#" + methodName + methodDescriptor);
		return methodNode;
	}

	public static MethodNode buildMethodNode(Method method) throws IOException, NoSuchMethodException {
		Class<?> klass = method.getParent().getBackingClass();
		String methodName = method.getName();
		MethodType internalType = method.getType();
		//Methods taking a this parameter have it explicitly represented in
		//their MethodType, but the JVM doesn't specify it in the method
		//descriptor.
		if (method.hasReceiver())
			internalType = internalType.dropFirstArgument();
		//We consider constructors to return their class (because the CallInst
		//defines a Value of that type), but the JVM thinks they return void.
		if (method.isConstructor())
			internalType = internalType.withReturnType(internalType.getTypeFactory().getType(void.class));
		String methodDescriptor = internalType.getDescriptor();
		return buildMethodNode(klass, methodName, methodDescriptor);
	}

	private static final class MethodNodeBuildingClassVisitor extends ClassVisitor {
		private final String name;
		private final String descriptor;
		private MethodNode mn;
		private MethodNodeBuildingClassVisitor(String name, String descriptor) {
			super(Opcodes.ASM4);
			this.name = name;
			this.descriptor = descriptor;
		}

		@Override
		public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
			if (name.equals(this.name) && desc.equals(this.descriptor)) {
				mn = new MethodNode(Opcodes.ASM4, access, name, desc, signature, exceptions);
				return mn;
			}
			return null;
		}

		public MethodNode getMethodNode() {
			return mn;
		}
	}
}
