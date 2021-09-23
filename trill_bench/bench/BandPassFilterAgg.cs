using System;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public struct LowPassAggState
    {
        public float[] InputStream; // Should always have size 12
        public float[] OutputStream; // Should always have size 2 
    }

    public class LowPassAgg : IAggregate<float, LowPassAggState, float>
    {
        public Expression<Func<LowPassAggState>> InitialState()
            => () => new LowPassAggState{ InputStream = new float[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                                          OutputStream = new float[] {0, 0}};

        public Expression<Func<LowPassAggState, long, float, LowPassAggState>> Accumulate()
            => (oldState, timestamp, input) =>
                new LowPassAggState {
                    InputStream = oldState.InputStream.Skip(1).Concat(new float[] {input}).ToArray(),
                    OutputStream = new float[] { oldState.OutputStream[1],
                                                 2 * oldState.OutputStream[1] - oldState.OutputStream[0] + input - 2 * oldState.InputStream[6] + oldState.InputStream[0]}
                };

        public Expression<Func<LowPassAggState, long, float, LowPassAggState>> Deaccumulate()
            => (oldState, timestamp, input) => oldState;

        public Expression<Func<LowPassAggState, LowPassAggState, LowPassAggState>> Difference()
            => (left, right) => left;

        public Expression<Func<LowPassAggState, float>> ComputeResult()
            => state => state.OutputStream[1];
    }

    public struct HighPassAggState
    {
        public float[] InputStream; // Should always have size 32
        public float Output; // Should always have size 
    }

    public class HighPassAgg : IAggregate<float, HighPassAggState, float>
    {
        public Expression<Func<HighPassAggState>> InitialState()
            => () => new HighPassAggState{ InputStream = new float[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                                      0, 0, 0, 0, 0, 0, 0, 0},
                                           Output = 0};

        public Expression<Func<HighPassAggState, long, float, HighPassAggState>> Accumulate()
            => (oldState, timestamp, input) =>
                new HighPassAggState {
                    InputStream = oldState.InputStream.Concat(new float[] {input}).Skip(1).ToArray(),
                    Output = 32 * oldState.InputStream[16] - (oldState.Output + input - oldState.InputStream[0])
                };

        public Expression<Func<HighPassAggState, long, float, HighPassAggState>> Deaccumulate()
            => (oldState, timestamp, input) => oldState;

        public Expression<Func<HighPassAggState, HighPassAggState, HighPassAggState>> Difference()
            => (left, right) => left;

        public Expression<Func<HighPassAggState, float>> ComputeResult()
            => state => state.Output;
    }
}
