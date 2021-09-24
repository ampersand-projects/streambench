using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public abstract class BandPassFilterAggregate<T> : IAggregate<T, List<Tuple<T, T>>, T>
    {
        public Expression<Func<List<Tuple<T, T>>>> InitialState() => () => new List<Tuple<T, T>>();

        protected abstract void UpdateList(List<Tuple<T, T>> set, long timestamp, T input);

        public Expression<Func<List<Tuple<T, T>>, long, T, List<Tuple<T, T>>>> Accumulate()
        {
            Expression<Action<List<Tuple<T, T>>, long, T>> temp = (set, timestamp, input) => UpdateList(set, timestamp, input);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<Tuple<T, T>>, long, T, List<Tuple<T, T>>>>(block, temp.Parameters);
        }

        public Expression<Func<List<Tuple<T, T>>, long, T, List<Tuple<T, T>>>> Deaccumulate()
        {
            Expression<Action<List<Tuple<T, T>>, long, T>> temp = (set, timestamp, input) => set.RemoveAt(0);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<Tuple<T, T>>, long, T, List<Tuple<T, T>>>>(block, temp.Parameters);
        }

        private static List<Tuple<T, T>> SetExcept(List<Tuple<T, T>> left, List<Tuple<T, T>> right)
        {
            foreach (var t in right) left.RemoveAt(0);
            return left;
        }

        public Expression<Func<List<Tuple<T, T>>, List<Tuple<T, T>>, List<Tuple<T, T>>>> Difference()
            => (leftSet, rightSet) => SetExcept(leftSet, rightSet);

        public Expression<Func<List<Tuple<T, T>>, T>> ComputeResult()
            => (state) => state[state.Count - 1].Item2;
    }

    public class LowPassFilterAggregate : BandPassFilterAggregate<float>
    {
        protected override void UpdateList(List<Tuple<float, float>> set, long timestamp, float input)
        {
            var output = input;
            if (set.Count > 0)
                output += 2 * set[set.Count - 1].Item2;
            if (set.Count > 1)
                output -= set[set.Count - 2].Item2;
            if (set.Count > 5)
                output -= 2 * set[set.Count - 6].Item1;
            if (set.Count > 11)
                output += set[set.Count - 12].Item1;
            set.Add(new Tuple<float, float>(input, output));
        }
    }

    public class HighPassFilterAggregate : BandPassFilterAggregate<float>
    {
        protected override void UpdateList(List<Tuple<float, float>> set, long timestamp, float input)
        {
            var output = -input;
            if (set.Count > 15)
                output += 32 * set[set.Count - 16].Item1;
            if (set.Count > 0)
                output -= set[set.Count - 1].Item2;
            if (set.Count > 31)
                output += set[set.Count - 32].Item1;

            set.Add(new Tuple<float, float>(input, output));
        }
    }

    public class DeriveAggregate : BandPassFilterAggregate<float>
    {
        private long window;
        public DeriveAggregate (long window) {
            this.window = window;
        }

        protected override void UpdateList(List<Tuple<float, float>> set, long timestamp, float input)
        {
            var output = input;
            if (set.Count > 3)
                output -= set[set.Count - 4].Item1;
            if (set.Count > 2)
                output -= 2 * set[set.Count - 3].Item1;
            if (set.Count > 1)
                output += 2 * set[set.Count - 1].Item1;
            output *= window / 8;
            
            set.Add(new Tuple<float, float>(input, output));
        }
    }
}
