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

    public abstract class BandPassFilterAggregate<T> : IAggregate<T, List<FilterState<T>>, T>
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
            foreach (var t in right) left.RemoveAt(0);
            return left;
        }

        public Expression<Func<List<FilterState<T>>, List<FilterState<T>>, List<FilterState<T>>>> Difference()
            => (leftSet, rightSet) => SetExcept(leftSet, rightSet);

        public Expression<Func<List<FilterState<T>>, T>> ComputeResult()
            => (state) => state[state.Count - 1].Output;
    }

    public class LowPassFilterAggregate : BandPassFilterAggregate<float>
    {
        protected override void UpdateList(List<FilterState<float>> set, long timestamp, float input)
        {
            var output = 0f;
            if (set.Count >= 12) {
                output = 2 * set[set.Count - 1].Output - set[set.Count - 2].Output 
                         + input - 2 * set[set.Count - 6].Input + set[set.Count - 12].Input;
            }
            set.Add(new FilterState<float>{Input = input, Output = output});
        }
    }

    public class HighPassFilterAggregate : BandPassFilterAggregate<float>
    {
        protected override void UpdateList(List<FilterState<float>> set, long timestamp, float input)
        {
            var output = 0f;
            if (set.Count >= 32) {
                output = 32 * set[set.Count - 16].Input - (set[set.Count - 1].Output + input - set[set.Count - 32].Input);
            }
            set.Add(new FilterState<float>{Input = input, Output = output});
        }
    }

    public class DeriveAggregate : BandPassFilterAggregate<float>
    {
        private long window;
        public DeriveAggregate (long window) {
            this.window = window;
        }

        protected override void UpdateList(List<FilterState<float>> set, long timestamp, float input)
        {
            var output = 0f;
            if (set.Count >= 4) {
                output = (window / 8) * (-set[set.Count - 4].Input - 2 * set[set.Count - 3].Input + 2 * set[set.Count - 1].Input + input);
            }
            set.Add(new FilterState<float>{Input = input, Output = output});
        }
    }
}
