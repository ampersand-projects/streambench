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

import static com.google.common.base.Preconditions.checkArgument;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.ReturnInst;
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import edu.mit.streamjit.util.bytecode.types.MethodType;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.util.bytecode.types.VoidType;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains static methods for common Method operations.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/3/2014
 */
public final class Methods {
	/**
	 * Trampoline for initializing static final fields.  This field is public so
	 * it can be accessed by arbitrary bytecode; it should not be modified
	 * directly.
	 */
	public static final Map<String, Object> TRAMPOLINE = new ConcurrentHashMap<>();
	private Methods() {}

	/**
	 * Creates a default (no-arg) constructor for the given class that simply
	 * calls the superclass default constructor and returns.
	 * @param klass the class to create a default constructor for
	 * @return the newly-created default constructor
	 */
	public static Method createDefaultConstructor(Klass klass) {
		Module module = klass.getParent();
		Method init = new Method("<init>",
				module.types().getMethodType(module.types().getType(klass)),
				EnumSet.of(Modifier.PUBLIC),
				klass);
		BasicBlock initBlock = new BasicBlock(init);
		Method objCtor = klass.getSuperclass().getMethods("<init>").iterator().next();
		initBlock.instructions().add(new CallInst(objCtor));
		initBlock.instructions().add(new ReturnInst(module.types().getVoidType()));
		return init;
	}

	/**
	 * Creates a class initializer ({@code <clinit>}) method for the given class
	 * that initializes static final fields of the given names with the
	 * corresponding values in the map.  The fields are of the most specific
	 * public class type.  (TODO: allow nonpublic class types or interface types
	 * if the best public type is Object?  general solution requires caller to
	 * pass in the type they want)
	 *
	 * The generated method depends on the live field values, and so cannot be
	 * usefully stored on disk or loaded in another VM.
	 * @param klass the class to create a class initializer method for
	 * @param fieldValues the values to initialize the fields with
	 * @return the newly-created class initializer method
	 */
	public static Method staticFinalFieldInitializer(Klass klass, Map<String, ?> fieldValues) {
		Module module = klass.getParent();
		Method clinitMethod = new Method("<clinit>", module.types().getMethodType("()V"), EnumSet.of(Modifier.STATIC), klass);
		BasicBlock clinit = new BasicBlock(clinitMethod);
		Field trampoline = module.getKlass(Methods.class).getField("TRAMPOLINE");
		LoadInst getstatic = new LoadInst(trampoline);
		clinit.instructions().add(getstatic);

		Method mapRemove = module.getKlass(Map.class).getMethod("remove", module.types().getMethodType(Object.class, Map.class, Object.class));
		fieldValues.forEach((name, value) -> {
			RegularType type = mostSpecificPublicClassType(module, value.getClass());
			Field field = new Field(type, name, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL), klass);
			//use a random UUID to avoid collisions
			String key = name+UUID.randomUUID().toString();
			TRAMPOLINE.put(key, value);
			CallInst remove = new CallInst(mapRemove, getstatic, module.constants().getConstant(key));
			CastInst cast = new CastInst(field.getType().getFieldType(), remove);
			StoreInst putstatic = new StoreInst(field, cast);
			clinit.instructions().addAll(Arrays.asList(remove, cast, putstatic));
		});

		clinit.instructions().add(new ReturnInst(module.types().getVoidType()));
		return clinitMethod;
	}

	private static RegularType mostSpecificPublicClassType(Module module, Class<?> c) {
		Klass klass = module.getKlass(c);
		while (klass.getBackingClass() != Object.class &&
				klass.getAccess() != Access.PUBLIC)
			klass = klass.getSuperclass();
		return module.types().getRegularType(klass);
	}

	/**
	 * Creates a method that loads a MethodHandle from a static field and
	 * invokes it on the method arguments (except the receiver, if the method is
	 * nonstatic).
	 *
	 * TODO: if the field and method are both nonstatic, load the field from the
	 * receiver?  (would be similar to MethodHandleProxies, though it wouldn't
	 * understand bridge methods, so...)
	 * @param klass the class to create the method in
	 * @param field the field to load the method handle from
	 * @param modifiers the new method's modifiers
	 * @param name the new method's name
	 * @param mhType the type of the method handle being invoked
	 * @return the newly-created method
	 */
	public static Method invokeExactFromField(Klass klass, Field field, Set<Modifier> modifiers, String name, java.lang.invoke.MethodType mhType) {
		checkArgument(field.getType().getFieldType().getKlass().getBackingClass() == MethodHandle.class,
				"not a MethodHandle-type field: %s", field);
		Module module = klass.getParent();
		MethodType type = module.types().getMethodType(mhType);
		if (!modifiers.contains(Modifier.STATIC))
			type = type.prependArgument(module.types().getRegularType(klass));
		Method n = new Method(name, type, modifiers, klass);
		BasicBlock block = new BasicBlock(n);
		LoadInst getHandle = new LoadInst(field);

		List<Value> invokeArgs = new ArrayList<>(n.arguments());
		MethodType callDescriptor = n.getType();
		if (n.hasReceiver()) {
			invokeArgs.remove(0);
			callDescriptor = callDescriptor.dropFirstArgument();
		}
		invokeArgs.add(0, getHandle);
		callDescriptor = callDescriptor.prependArgument(module.types().getRegularType(MethodHandle.class));

		Method invokeExact = module.getKlass(MethodHandle.class).getMethod("invokeExact", module.types().getMethodType(Object.class, MethodHandle.class, Object[].class));
		CallInst invoke = new CallInst(invokeExact, callDescriptor, invokeArgs.toArray(new Value[0]));
		//TODO: allow passing void value to return (just ignore it)
		ReturnInst ret = invoke.getType() instanceof VoidType ?
				new ReturnInst(invoke.getType()) :
				new ReturnInst(invoke.getType(), invoke);
		block.instructions().addAll(Arrays.asList(getHandle, invoke, ret));
		return n;
	}
}
