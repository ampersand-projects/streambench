/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.util.bytecode.methodhandles;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.Primitives;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findStatic;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.param;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/6/2014 (some methods from StreamJIT @since 10/3/2013)
 */
public final class Combinators {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private Combinators() {throw new AssertionError();}

	private static final MethodHandle VOID_VOID_NOP = MethodHandles.identity(Object.class)
			.bindTo(null).asType(MethodType.methodType(void.class));
	/**
	 * Returns a method handle that takes the given argument types, returns
	 * void, and does nothing when called.
	 * @param arguments the argument types
	 * @return a no-op method handle
	 */
	public static MethodHandle nop(Class<?>... arguments) {
		return MethodHandles.dropArguments(VOID_VOID_NOP, 0, arguments);
	}

	/**
	 * Returns a method handle that calls the given method handles in sequence,
	 * ignoring their return values.  The given handles must all take the same
	 * parameters.  They may have any return type, but any returned values will
	 * be ignored.  If no handles are given, the returned handle does nothing.
	 * @param handles the handles to invoke
	 * @return a method handle approximating semicolons
	 */
	public static MethodHandle semicolon(MethodHandle... handles) {
		if (handles.length == 0)
			return nop();
		MethodType type = handles[0].type().changeReturnType(void.class);
		if (handles.length == 1)
			return handles[0].asType(type);
		MethodHandle chain = nop(type.parameterArray());
		for (int i = handles.length-1; i >= 0; --i) {
			checkArgument(handles[i].type().parameterList().equals(type.parameterList()), "type mismatch in %s", Arrays.asList(handles));
			chain = MethodHandles.foldArguments(chain, handles[i].asType(type));
		}
		return chain;
	}

	/**
	 * Returns a method handle that calls the given method handles in sequence,
	 * ignoring their return values.  The given handles must all take the same
	 * parameters.  They may have any return type, but any returned values will
	 * be ignored.  If no handles are given, the returned handle does nothing.
	 * @param handles the handles to invoke
	 * @return a method handle approximating semicolons
	 */
	public static MethodHandle semicolon(List<MethodHandle> handles) {
		return semicolon(handles.toArray(new MethodHandle[0]));
	}

	private static final MethodHandle METHODHANDLE_ARRAY_GETTER = MethodHandles.arrayElementGetter(MethodHandle[].class);
	/**
	 * Returns a method handle with a leading int argument that selects one of
	 * the method handles in the given array, which is invoked with the
	 * remaining arguments. Modifications to the array after this method returns
	 * do not affect the behavior of the returned handle.
	 * @param cases the cases to select from
	 * @return a method handle approximating the switch statement
	 */
	public static MethodHandle tableswitch(MethodHandle[] cases) {
		checkArgument(cases.length >= 1);
		MethodType type = cases[0].type();
		for (MethodHandle mh : cases)
			checkArgument(mh.type().equals(type), "type mismatch in %s", Arrays.asList(cases));
		MethodHandle selector = METHODHANDLE_ARRAY_GETTER.bindTo(cases.clone());
		//Replace the index with the handle to invoke, passing it to an invoker.
		return MethodHandles.filterArguments(MethodHandles.exactInvoker(type), 0, selector);
	}

	/**
	 * Returns a method handle with a leading int argument that selects one of
	 * the method handles in the given map, which is invoked with the remaining
	 * arguments.  If the leading int argument is not present in the map, the
	 * given default handle is executed instead with the leading int argument
	 * and any remaining arguments.
	 * @param cases the switch cases
	 * @param defaultCase the handle to call if no cases match
	 * @return a method handle approximating the switch statement
	 */
	public static MethodHandle lookupswitch(Map<Integer, MethodHandle> cases, MethodHandle defaultCase) {
		return lookupswitch(ImmutableSortedMap.copyOf(cases), defaultCase);
	}

	private static final MethodHandle INTCONSUMER_ACCEPT = findVirtual(IntConsumer.class, "accept");
	/**
	 * Returns a method handle with a leading int argument that selects one of
	 * the method handles in the given map, which is invoked with the remaining
	 * arguments.  If the leading int argument is not present in the map, an
	 * AssertionError will be thrown.
	 * @param cases the switch cases
	 * @return a method handle approximating the switch statement
	 */
	public static MethodHandle lookupswitch(Map<Integer, MethodHandle> cases) {
		ImmutableSortedMap<Integer, MethodHandle> sortedCases = ImmutableSortedMap.copyOf(cases);
		String validCases = sortedCases.keySet().toString();
		IntConsumer defaultCaseAction = (idx) -> {throw new AssertionError(
				String.format("lookupswitch index %d not in cases %s", idx, validCases));};
		MethodHandle defaultCase = INTCONSUMER_ACCEPT.bindTo(defaultCaseAction);
		if (!sortedCases.values().isEmpty()) {
			//just pick an arbitrary element -- we'll catch type mismatches later
			MethodType t = sortedCases.values().iterator().next().type();
			defaultCase = MethodHandles.dropArguments(defaultCase, 1, t.parameterArray());
			defaultCase = defaultCase.asType(defaultCase.type().changeReturnType(t.returnType()));
		}
		return lookupswitch(sortedCases, defaultCase);
	}

	/**
	 * Returns a method handle with a leading int argument that selects one of
	 * the method handles in the given array, which is invoked with the
	 * remaining arguments. Modifications to the array after this method returns
	 * do not affect the behavior of the returned handle.
	 * @param cases the switch cases
	 * @return a method handle approximating the switch statement
	 * @see #tableswitch(java.lang.invoke.MethodHandle[])
	 */
	public static MethodHandle lookupswitch(MethodHandle[] cases) {
		ImmutableSortedMap.Builder<Integer, MethodHandle> casesMap = ImmutableSortedMap.naturalOrder();
		for (int i = 0; i < cases.length; ++i)
			casesMap.put(i, cases[i]);
		return lookupswitch(casesMap.build());
	}

	private static MethodHandle lookupswitch(ImmutableSortedMap<Integer, MethodHandle> cases, MethodHandle defaultCase) {
		if (cases.isEmpty()) {
			checkArgument(defaultCase.type().parameterList().equals(ImmutableList.of(int.class)),
					"bad type for default case %s", defaultCase.type());
			return defaultCase;
		}
		MethodType type = cases.values().iterator().next().type();
		for (MethodHandle mh : cases.values())
			checkArgument(mh.type().equals(type), "type mismatch in %s", cases.values());
		checkArgument(defaultCase.type().equals(type.insertParameterTypes(0, int.class)),
				"bad type for default case %s, other cases %s", defaultCase.type(), type);
		return lookupswitch0(cases, defaultCase);
	}

	private static MethodHandle lookupswitch0(ImmutableSortedMap<Integer, MethodHandle> cases, MethodHandle defaultCase) {
		if (cases.isEmpty())
			return defaultCase;
		if (cases.size() == 1) {
			Map.Entry<Integer, MethodHandle> next = cases.entrySet().iterator().next();
			return MethodHandles.guardWithTest(eq(next.getKey()),
					MethodHandles.dropArguments(next.getValue(), 0, int.class), //discard the case index
					defaultCase);
		}
		int median = median(cases.keySet().asList());
		return MethodHandles.guardWithTest(le(median),
				lookupswitch0(cases.headMap(median, true), defaultCase),
				lookupswitch0(cases.tailMap(median, false), defaultCase));
	}

	private static int median(List<Integer> list) {
		if (list.size() % 2 == 1)
			return list.get(list.size()/2);
		return (list.get(list.size()/2) + list.get(list.size()/2 - 1))/2;
	}

	/**
	 * Returns a method handle that invokes the target handle with the result of
	 * invoking the given argument handles.
	 * @param target the method handle to invoke
	 * @param args the method handles returning arguments for the target
	 * @return a method handle that invokes the target with the result of
	 * invoking the given argument handles
	 */
	public static MethodHandle apply(MethodHandle target, MethodHandle... args) {
		return apply(target, Arrays.asList(args));
	}

	/**
	 * Returns a method handle that invokes the target handle with the result of
	 * invoking the given argument handles.
	 * @param target the method handle to invoke
	 * @param args the method handles returning arguments for the target
	 * @return a method handle that invokes the target with the result of
	 * invoking the given argument handles
	 */
	public static MethodHandle apply(MethodHandle target, Iterable<MethodHandle> args) {
		return apply(target, args.iterator());
	}

	/**
	 * Returns a method handle that invokes the target handle with the result of
	 * invoking the given argument handles.
	 * @param target the method handle to invoke
	 * @param args the method handles returning arguments for the target
	 * @return a method handle that invokes the target with the result of
	 * invoking the given argument handles
	 */
	public static MethodHandle apply(MethodHandle target, Iterator<MethodHandle> args) {
		while (args.hasNext()) {
			MethodHandle a = args.next();
			target = MethodHandles.collectArguments(target, 0,
					a.asType(a.type().changeReturnType(target.type().parameterType(0))));
		}
		return target;
	}

	private static int _arraylength(boolean[] a) {
		return a.length;
	}
	private static int _arraylength(byte[] a) {
		return a.length;
	}
	private static int _arraylength(short[] a) {
		return a.length;
	}
	private static int _arraylength(char[] a) {
		return a.length;
	}
	private static int _arraylength(int[] a) {
		return a.length;
	}
	private static int _arraylength(long[] a) {
		return a.length;
	}
	private static int _arraylength(float[] a) {
		return a.length;
	}
	private static int _arraylength(double[] a) {
		return a.length;
	}
	private static int _arraylength(Object[] a) {
		return a.length;
	}
	private static final MethodHandle REFERENCE_ARRAYLENGTH = findStatic(LOOKUP, "_arraylength", param(0, Object[].class));
	private static final ImmutableMap<Class<?>, MethodHandle> PRIMITIVE_ARRAYLENGTH;
	static {
		ImmutableMap.Builder<Class<?>, MethodHandle> builder = ImmutableMap.builder();
		for (Class<?> c : Primitives.allPrimitiveTypes())
			if (c != void.class) {
				Class<?> arrayClass = Array.newInstance(c, 0).getClass();
				builder.put(arrayClass, findStatic(LOOKUP, Combinators.class, "_arraylength", param(0, arrayClass)));
			}
		PRIMITIVE_ARRAYLENGTH = builder.build();
	}
	/**
	 * Returns a method handle taking one argument of the given array type and
	 * returning its length (as an int).
	 * @param arrayClass an array type
	 * @return a method handle returning the length of arrays of the given type
	 */
	public static MethodHandle arraylength(Class<?> arrayClass) {
		checkArgument(arrayClass.isArray(), "%s not an array class", arrayClass);
		if (arrayClass.getComponentType().isPrimitive()) {
			MethodHandle handle = PRIMITIVE_ARRAYLENGTH.get(arrayClass);
			assert handle != null : arrayClass;
			return handle;
		}
		return REFERENCE_ARRAYLENGTH.asType(MethodType.methodType(int.class, arrayClass));
	}

	private static final MethodHandle INTPREDICATE_TEST = findVirtual(IntPredicate.class, "test");
	//TODO: make these public if we find another use
	//TODO: the lambda desugars to a static method; should we make those methods
	//explicit to avoid creating the lambda objects?
	private static MethodHandle eq(int y) {
		return INTPREDICATE_TEST.bindTo((IntPredicate)(x -> x == y));
	}

	private static MethodHandle le(int y) {
		return INTPREDICATE_TEST.bindTo((IntPredicate)(x -> x <= y));
	}

	private static final MethodHandle INTEGER_SUM = findStatic(Integer.class, "sum");
	/**
	 * Returns an int -> int method handle that returns the sum of its argument
	 * and the given addend.
	 *
	 * TODO: it's not clear what the proper interface for math combinators is,
	 * or if they're generally useful.  StreamJIT only needs this one (and sub
	 * which justs uses negative addends).
	 * @param addend the addend to add
	 * @return a method handle that adds the given addend
	 */
	public static MethodHandle adder(int addend) {
		return MethodHandles.insertArguments(INTEGER_SUM, 1, addend);
	}
}
