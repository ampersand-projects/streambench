using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public struct AverageState
    {
        public float Sum;
        public float Square;
        public ulong Count;
    }

    public struct ZScore
    {
        public float avg;
        public float stddev;
    }

    public class ZScoreAgg : ISummableAggregate<float, AverageState, ZScore>
    {
        public Expression<Func<AverageState>> InitialState()
            => () => default;

        public Expression<Func<AverageState, long, float, AverageState>> Accumulate()
            => (oldState, timestamp, input) => new AverageState
            {
                Count = oldState.Count + 1, Sum = oldState.Sum + input, Square = oldState.Square + input * input
            };

        public Expression<Func<AverageState, long, float, AverageState>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState
            {
                Count = oldState.Count - 1, Sum = oldState.Sum - input, Square = oldState.Square - input * input
            };

        public Expression<Func<AverageState, AverageState, AverageState>> Difference()
            => (left, right) => new AverageState
            {
                Count = left.Count - right.Count, Sum = left.Sum - right.Sum, Square = left.Square - right.Square
            };

        public Expression<Func<AverageState, AverageState, AverageState>> Sum()
            => (left, right) => new AverageState
            {
                Count = left.Count + right.Count, Sum = left.Sum + right.Sum, Square = left.Square + right.Square
            };

        public Expression<Func<AverageState, ZScore>> ComputeResult()
            => state => new ZScore
            {
                avg = state.Sum / state.Count,
                stddev = (state.Square / state.Count) - (state.Sum / state.Count) * (state.Sum / state.Count)
            };
    }

}
