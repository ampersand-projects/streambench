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
package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Collections2;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.util.ReflectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The compiler IR for a Worker or Token.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 9/21/2013
 */
public abstract class Actor implements Comparable<Actor> {
	private ActorGroup group;
	/**
	 * The upstream and downstream Storage, one for each input or output of this
	 * Actor.  TokenActors will have either inputs xor outputs.
	 */
	private final ArrayList<Storage> upstream = new ArrayList<>(), downstream = new ArrayList<>();
	/**
	 * Index functions (int -> int) that transform a nominal index
	 * (iteration * rate + popCount/pushCount (+ peekIndex)) into a physical
	 * index (subject to further adjustment if circular buffers are in use).
	 * One for each input or output of this actor.
	 */
	private final ArrayList<IndexFunction> upstreamIndex = new ArrayList<>(),
			downstreamIndex = new ArrayList<>();
	/**
	 * Liveness information for the Storage on the inputs of this actor.  Lazily
	 * initialized in inputSlots.
	 */
	private ArrayList<StorageSlotList> inputSlots;
	private TypeToken<?> inputType, outputType;
	protected Actor(TypeToken<?> inputType, TypeToken<?> outputType) {
		//It would be technically more correct to create fresh type variables
		//for each actor, but that should never matter so long as we only care
		//about wrapper types.
		this.inputType = inputType;
		this.outputType = outputType;
	}

	public abstract int id();

	public ActorGroup group() {
		return group;
	}

	void setGroup(ActorGroup group) {
		assert ReflectionUtils.calledDirectlyFrom(ActorGroup.class);
		this.group = group;
	}

	public final boolean isPeeking() {
		for (int i = 0; i < inputs().size(); ++i)
			if (peek(i) > pop(i))
				return true;
		return false;
	}

	public TypeToken<?> inputType() {
		return inputType;
	}

	public void setInputType(TypeToken<?> type) {
		this.inputType = type;
	}

	public TypeToken<?> outputType() {
		return outputType;
	}

	public void setOutputType(TypeToken<?> type) {
		this.outputType = type;
	}

	public List<Storage> inputs() {
		return upstream;
	}

	public List<Storage> outputs() {
		return downstream;
	}

	public abstract int peek(int input);

	public abstract int pop(int input);

	public abstract int push(int output);

	/**
	 * Returns the number of items peeked at but not popped from the given input
	 * in a single iteration.
	 * @param input the input index
	 * @return the number of items peeked but not popped
	 */
	public int excessPeeks(int input) {
		return Math.max(0, peek(input) - pop(input));
	}

	/**
	 * Returns the logical indices peeked or popped on the given input during
	 * the given iteration.  Note that this method may return a nonempty set
	 * even if peeks(input) returns 0 and isPeeking() returns false.
	 * @param input the input index
	 * @param iteration the iteration number
	 * @return the logical indices peeked or popped on the given input during
	 * the given iteration
	 */
	public ContiguousSet<Integer> peeks(int input, int iteration) {
		return ContiguousSet.create(Range.closedOpen(iteration * pop(input), (iteration + 1) * pop(input) + excessPeeks(input)), DiscreteDomain.integers());
	}

	/**
	 * Returns the logical indices peeked or popped on the given input during
	 * the given iterations.  Note that this method may return a nonempty set
	 * even if peeks(input) returns 0 and isPeeking() returns false.
	 * @param input the input index
	 * @param iterations the iteration numbers
	 * @return the logical indices peeked or popped on the given input during
	 * the given iterations
	 */
	public ImmutableSortedSet<Integer> peeks(int input, Set<Integer> iterations) {
		if (iterations.isEmpty()) return ImmutableSortedSet.of();
		if (iterations instanceof ContiguousSet)
			return peeks(input, (ContiguousSet<Integer>)iterations);
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (int i : iterations)
			builder.addAll(peeks(input, i));
		return builder.build();
	}

	/**
	 * Returns the logical indices peeked or popped on the given input during
	 * the given iterations.  Note that this method may return a nonempty set
	 * even if peeks(input) returns 0 and isPeeking() returns false.
	 * @param input the input index
	 * @param iterations the iteration numbers
	 * @return the logical indices peeked or popped on the given input during
	 * the given iterations
	 */
	public ContiguousSet<Integer> peeks(int input, ContiguousSet<Integer> iterations) {
		if (iterations.isEmpty()) return ContiguousSet.create(Range.closedOpen(0, 0), DiscreteDomain.integers());
		return ContiguousSet.create(Range.closedOpen(iterations.first() * pop(input), (iterations.last() + 1) * pop(input) + excessPeeks(input)), DiscreteDomain.integers());
	}

	/**
	 * Returns the logical indices peeked or popped on the given input during
	 * the given iterations.  Note that this method may return a nonempty set
	 * even if peeks(input) returns 0 and isPeeking() returns false.
	 * @param input the input index
	 * @param iterations the iteration numbers
	 * @return the logical indices peeked or popped on the given input during
	 * the given iterations
	 */
	public ContiguousSet<Integer> peeks(int input, Range<Integer> iterations) {
		return peeks(input, ContiguousSet.create(iterations, DiscreteDomain.integers()));
	}

	/**
	 * Returns the logical indices popped on the given input during the given
	 * iteration.
	 * @param input the input index
	 * @param iteration the iteration number
	 * @return the logical indices popped on the given input during the given
	 * iteration
	 */
	public ContiguousSet<Integer> pops(int input, int iteration) {
		return ContiguousSet.create(Range.closedOpen(iteration * pop(input), (iteration + 1) * pop(input)), DiscreteDomain.integers());
	}

	/**
	 * Returns the logical indices popped on the given input during the given
	 * iteration.
	 * @param input the input index
	 * @param iterations the iteration numbers
	 * @return the logical indices popped on the given input during the given
	 * iterations
	 */
	public ImmutableSortedSet<Integer> pops(int input, Set<Integer> iterations) {
		if (iterations.isEmpty()) return ImmutableSortedSet.of();
		if (iterations instanceof ContiguousSet)
			return pops(input, (ContiguousSet<Integer>)iterations);
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (int i : iterations)
			builder.addAll(pops(input, i));
		return builder.build();
	}

	/**
	 * Returns the logical indices popped on the given input during the given
	 * iteration.
	 * @param input the input index
	 * @param iterations the iteration numbers
	 * @return the logical indices popped on the given input during the given
	 * iterations
	 */
	public ContiguousSet<Integer> pops(int input, ContiguousSet<Integer> iterations) {
		if (iterations.isEmpty()) return ContiguousSet.create(Range.closedOpen(0, 0), DiscreteDomain.integers());
		return ContiguousSet.create(Range.closedOpen(iterations.first() * pop(input), (iterations.last() + 1) * pop(input)), DiscreteDomain.integers());
	}

	/**
	 * Returns the logical indices popped on the given input during the given
	 * iteration.
	 * @param input the input index
	 * @param iterations the iteration numbers
	 * @return the logical indices popped on the given input during the given
	 * iterations
	 */
	public ContiguousSet<Integer> pops(int input, Range<Integer> iterations) {
		return pops(input, ContiguousSet.create(iterations, DiscreteDomain.integers()));
	}

	/**
	 * Returns the logical indices pushed to the given output during the given
	 * iteration.
	 * @param output the output index
	 * @param iteration the iteration number
	 * @return the logical indices pushed to the given input during the given
	 * iteration
	 */
	public ContiguousSet<Integer> pushes(int output, int iteration) {
		return ContiguousSet.create(Range.closedOpen(iteration * push(output), (iteration + 1) * push(output)), DiscreteDomain.integers());
	}

	/**
	 * Returns the logical indices pushed to the given output during the given
	 * iterations.
	 * @param output the output index
	 * @param iterations the iteration numbers
	 * @return the logical indices pushed to the given input during the given
	 * iterations
	 */
	public ImmutableSortedSet<Integer> pushes(int output, Set<Integer> iterations) {
		if (iterations.isEmpty()) return ImmutableSortedSet.of();
		if (iterations instanceof ContiguousSet)
			return pushes(output, (ContiguousSet<Integer>)iterations);
		ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
		for (int i : iterations)
			builder.addAll(pushes(output, i));
		return builder.build();
	}

	/**
	 * Returns the logical indices pushed to the given output during the given
	 * iterations.
	 * @param output the output index
	 * @param iterations the iteration numbers
	 * @return the logical indices pushed to the given input during the given
	 * iterations
	 */
	public ContiguousSet<Integer> pushes(int output, ContiguousSet<Integer> iterations) {
		if (iterations.isEmpty()) return ContiguousSet.create(Range.closedOpen(0, 0), DiscreteDomain.integers());
		return ContiguousSet.create(Range.closedOpen(iterations.first() * push(output), (iterations.last() + 1) * push(output)), DiscreteDomain.integers());
	}

	/**
	 * Returns the logical indices pushed to the given output during the given
	 * iterations.
	 * @param output the output index
	 * @param iterations the iteration numbers
	 * @return the logical indices pushed to the given input during the given
	 * iterations
	 */
	public ContiguousSet<Integer> pushes(int output, Range<Integer> iterations) {
		return pushes(output, ContiguousSet.create(iterations, DiscreteDomain.integers()));
	}

	public List<IndexFunction> inputIndexFunctions() {
		return upstreamIndex;
	}

	public int translateInputIndex(int input, int logicalIndex) {
		checkArgument(logicalIndex >= 0);
		IndexFunction idxFxn = upstreamIndex.get(input);
		try {
			return idxFxn.applyAsInt(logicalIndex);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("index functions should not throw; translateInputIndex(%d, %d)", input, logicalIndex), ex);
		}
	}

	public ImmutableSortedSet<Integer> translateInputIndices(final int input, Set<Integer> logicalIndices) {
		return ImmutableSortedSet.copyOf(Collections2.transform(logicalIndices, index -> translateInputIndex(input, index)));
	}

	public ImmutableSortedSet<Integer> translateInputIndices(final int input, Range<Integer> logicalIndices) {
		return translateInputIndices(input, ContiguousSet.create(logicalIndices, DiscreteDomain.integers()));
	}

	public List<IndexFunction> outputIndexFunctions() {
		return downstreamIndex;
	}

	public int translateOutputIndex(int output, int logicalIndex) {
		checkArgument(logicalIndex >= 0);
		IndexFunction idxFxn = downstreamIndex.get(output);
		try {
			return idxFxn.applyAsInt(logicalIndex);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("index functions should not throw; translateOutputIndex(%d, %d)", output, logicalIndex), ex);
		}
	}

	public ImmutableSortedSet<Integer> translateOutputIndices(final int input, Set<Integer> logicalIndices) {
		return ImmutableSortedSet.copyOf(Collections2.transform(logicalIndices, index -> translateOutputIndex(input, index)));
	}

	public ImmutableSortedSet<Integer> translateOutputIndices(final int input, Range<Integer> logicalIndices) {
		return translateOutputIndices(input, ContiguousSet.create(logicalIndices, DiscreteDomain.integers()));
	}

	public ImmutableSortedSet<Integer> reads(int input, int iteration) {
		return translateInputIndices(input, peeks(input, iteration));
	}

	public ImmutableSortedSet<Integer> reads(int input, Set<Integer> iterations) {
		return translateInputIndices(input, peeks(input, iterations));
	}

	public ImmutableSortedSet<Integer> reads(int input, Range<Integer> iterations) {
		return translateInputIndices(input, peeks(input, iterations));
	}

	public ImmutableSortedSet<Integer> reads(Storage storage, int iteration) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(inputs().size());
		for (int input = 0; input < inputs().size(); ++input)
			if (inputs().get(input).equals(storage))
				list.add(translateInputIndices(input, peeks(input, iteration)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableSortedSet<Integer> reads(Storage storage, Set<Integer> iterations) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(inputs().size());
		for (int input = 0; input < inputs().size(); ++input)
			if (inputs().get(input).equals(storage))
				list.add(translateInputIndices(input, peeks(input, iterations)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableSortedSet<Integer> reads(Storage storage, Range<Integer> iterations) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(inputs().size());
		for (int input = 0; input < inputs().size(); ++input)
			if (inputs().get(input).equals(storage))
				list.add(translateInputIndices(input, peeks(input, iterations)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> reads(int iteration) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : inputs())
			builder.put(s, reads(s, iteration));
		return builder.build();
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> reads(Set<Integer> iterations) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : inputs())
			builder.put(s, reads(s, iterations));
		return builder.build();
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> reads(Range<Integer> iterations) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : inputs())
			builder.put(s, reads(s, iterations));
		return builder.build();
	}

	public ImmutableSortedSet<Integer> consumes(int input, int iteration) {
		return translateInputIndices(input, pops(input, iteration));
	}

	public ImmutableSortedSet<Integer> consumes(int input, Set<Integer> iterations) {
		return translateInputIndices(input, pops(input, iterations));
	}

	public ImmutableSortedSet<Integer> consumes(int input, Range<Integer> iterations) {
		return translateInputIndices(input, pops(input, iterations));
	}

	public ImmutableSortedSet<Integer> consumes(Storage storage, int iteration) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(inputs().size());
		for (int input = 0; input < inputs().size(); ++input)
			if (inputs().get(input).equals(storage))
				list.add(translateInputIndices(input, pops(input, iteration)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableSortedSet<Integer> consumes(Storage storage, Set<Integer> iterations) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(inputs().size());
		for (int input = 0; input < inputs().size(); ++input)
			if (inputs().get(input).equals(storage))
				list.add(translateInputIndices(input, pops(input, iterations)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableSortedSet<Integer> consumes(Storage storage, Range<Integer> iterations) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(inputs().size());
		for (int input = 0; input < inputs().size(); ++input)
			if (inputs().get(input).equals(storage))
				list.add(translateInputIndices(input, pops(input, iterations)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> consumes(int iteration) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : inputs())
			builder.put(s, consumes(s, iteration));
		return builder.build();
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> consumes(Set<Integer> iterations) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : inputs())
			builder.put(s, consumes(s, iterations));
		return builder.build();
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> consumes(Range<Integer> iterations) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : inputs())
			builder.put(s, consumes(s, iterations));
		return builder.build();
	}

	public ImmutableSortedSet<Integer> writes(int output, int iteration) {
		return translateOutputIndices(output, pushes(output, iteration));
	}

	public ImmutableSortedSet<Integer> writes(int output, Set<Integer> iterations) {
		return translateOutputIndices(output, pushes(output, iterations));
	}

	public ImmutableSortedSet<Integer> writes(int output, Range<Integer> iterations) {
		return translateOutputIndices(output, pushes(output, iterations));
	}

	public ImmutableSortedSet<Integer> writes(Storage storage, int iteration) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(outputs().size());
		for (int output = 0; output < outputs().size(); ++output)
			if (outputs().get(output).equals(storage))
				list.add(translateOutputIndices(output, pushes(output, iteration)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableSortedSet<Integer> writes(Storage storage, Set<Integer> iterations) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(outputs().size());
		for (int output = 0; output < outputs().size(); ++output)
			if (outputs().get(output).equals(storage))
				list.add(translateOutputIndices(output, pushes(output, iterations)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableSortedSet<Integer> writes(Storage storage, Range<Integer> iterations) {
		List<ImmutableSortedSet<Integer>> list = new ArrayList<>(outputs().size());
		for (int output = 0; output < outputs().size(); ++output)
			if (outputs().get(output).equals(storage))
				list.add(translateOutputIndices(output, pushes(output, iterations)));
		return ImmutableSortedSet.copyOf(Iterables.concat(list));
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> writes(int iteration) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : outputs())
			builder.put(s, writes(s, iteration));
		return builder.build();
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> writes(Set<Integer> iterations) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : outputs())
			builder.put(s, writes(s, iterations));
		return builder.build();
	}

	public ImmutableMap<Storage, ImmutableSortedSet<Integer>> writes(Range<Integer> iterations) {
		ImmutableMap.Builder<Storage, ImmutableSortedSet<Integer>> builder = ImmutableMap.builder();
		for (Storage s : outputs())
			builder.put(s, writes(s, iterations));
		return builder.build();
	}

	public StorageSlotList inputSlots(int input) {
		if (inputSlots == null) {
			inputSlots = new ArrayList<>(inputs().size());
			for (int i = 0; i < inputs().size(); ++i)
				inputSlots.add(new StorageSlotList());
		}
		return inputSlots.get(input);
	}

	@Override
	public final int compareTo(Actor o) {
		return Integer.compare(id(), o.id());
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Actor other = (Actor)obj;
		if (id() != other.id())
			return false;
		return true;
	}

	@Override
	public final int hashCode() {
		return id();
	}
}
