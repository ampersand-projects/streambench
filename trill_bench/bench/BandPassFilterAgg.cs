using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public struct FilterState<T>
    {
        public T Input;
        public T Output;
    }

    public abstract class InputOutputListAggregate<T> : IAggregate<T, List<FilterState<T>>, T>
    {   
        public Expression<Func<List<FilterState<T>>>> InitialState() => () => new List<FilterState<T>>();

        protected abstract void UpdateList(List<FilterState<T>> set, long timestamp, T input);

        public Expression<Func<List<FilterState<T>>, long, T, List<FilterState<T>>>> Accumulate()
        {
            Expression<Action<List<FilterState<T>>, long, T>> temp = (set, timestamp, input) => UpdateList(set, timestamp, input);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<FilterState<T>>, long, T, List<FilterState<T>>>>(block, temp.Parameters);
        }

        public Expression<Func<List<FilterState<T>>, long, T, List<FilterState<T>>>> Deaccumulate()
        {
            Expression<Action<List<FilterState<T>>, long, T>> temp = (set, timestamp, input) => set.RemoveAt(0);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<FilterState<T>>, long, T, List<FilterState<T>>>>(block, temp.Parameters);
        }

        private static List<FilterState<T>> SetExcept(List<FilterState<T>> left, List<FilterState<T>> right)
        {
            left.RemoveRange(0, right.Count);
            return left;
        }

        public Expression<Func<List<FilterState<T>>, List<FilterState<T>>, List<FilterState<T>>>> Difference()
            => (leftSet, rightSet) => SetExcept(leftSet, rightSet);

        public Expression<Func<List<FilterState<T>>, T>> ComputeResult()
            => (state) => state[state.Count - 1].Output;
    }

    public abstract class InputOutputFloatListAggregate : InputOutputListAggregate<float>
    {
        protected FilterState<float> GetElementFromBack(List<FilterState<float>> set, int idx)
        {
            if (set.Count >= idx)
                return set[set.Count - idx];
            return new FilterState<float> {Input = 0f, Output = 0f};
        }
    }

    public class LowPassFilterAggregate : InputOutputFloatListAggregate
    {
        protected override void UpdateList(List<FilterState<float>> set, long timestamp, float input)
        {
            var output = 2 * GetElementFromBack(set, 1).Output - GetElementFromBack(set, 2).Output + input
                         - 2 * GetElementFromBack(set, 6).Input + GetElementFromBack(set, 12).Input;
            set.Add(new FilterState<float>{Input = input, Output = output});
        }
    }

    public class HighPassFilterAggregate : InputOutputFloatListAggregate
    {
        protected override void UpdateList(List<FilterState<float>> set, long timestamp, float input)
        {
            var output = 32 * GetElementFromBack(set, 16).Input - (GetElementFromBack(set, 1).Output
                         + input - GetElementFromBack(set, 32).Input);
            set.Add(new FilterState<float>{Input = input, Output = output});
        }
    }

    public class DeriveAggregate : InputOutputFloatListAggregate
    {
        private long period;
        public DeriveAggregate (long period) {
            this.period = period;
        }

        protected override void UpdateList(List<FilterState<float>> set, long timestamp, float input)
        {
            var output = (period / 8) * (- GetElementFromBack(set, 4).Input - 2 * GetElementFromBack(set, 3).Input 
                                         + 2 * GetElementFromBack(set, 1).Input + input);
            set.Add(new FilterState<float>{Input = input, Output = output});
        }
    }
}
