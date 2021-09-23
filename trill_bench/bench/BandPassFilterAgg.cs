using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    public struct PassAggState
    {
        public List<float> InputStream;
        public List<float> OutputStream;
    }

    public class LowPassAgg : IAggregate<float, PassAggState, float>
    {
        public Expression<Func<PassAggState>> InitialState()
            => () => new PassAggState
            { 
                InputStream = new List<float>(Enumerable.Repeat(0f, 12)),
                OutputStream = new List<float>(Enumerable.Repeat(0f, 2))
            };

        public static PassAggState accumulate(PassAggState oldState, long timestamp, float input)
        {
            var oldput_payload = 2 * oldState.OutputStream[1] - oldState.OutputStream[0] + input - 2 * oldState.InputStream[6] + oldState.InputStream[0];
            oldState.OutputStream.Add(oldput_payload);
            oldState.OutputStream.RemoveAt(0);
            oldState.InputStream.Add(input);
            oldState.InputStream.RemoveAt(0);
            return oldState;
        }       

        public Expression<Func<PassAggState, long, float, PassAggState>> Accumulate()
            => (oldState, timestamp, input) => accumulate(oldState, timestamp, input);

        public Expression<Func<PassAggState, long, float, PassAggState>> Deaccumulate()
            => (oldState, timestamp, input) => accumulate(oldState, timestamp, input);

        public Expression<Func<PassAggState, PassAggState, PassAggState>> Difference()
            => (left, right) => left;

        public Expression<Func<PassAggState, float>> ComputeResult()
            => state => state.OutputStream[1];
    }

    public class HighPassAgg : IAggregate<float, PassAggState, float>
    {
        public Expression<Func<PassAggState>> InitialState()
            => () => new PassAggState
            {
                InputStream = new List<float>(new float[32]),
                OutputStream = new List<float>(new float[1])
            };

        public static PassAggState accumulate(PassAggState oldState, long timestamp, float input)
        {
            var oldput_payload = 32 * oldState.InputStream[16] - (oldState.OutputStream[0] + input - oldState.InputStream[0]);
            oldState.OutputStream.Add(oldput_payload);
            oldState.OutputStream.RemoveAt(0);
            oldState.InputStream.Add(input);
            oldState.InputStream.RemoveAt(0);
            return oldState;
        }  

        public Expression<Func<PassAggState, long, float, PassAggState>> Accumulate()
            => (oldState, timestamp, input) => accumulate(oldState, timestamp, input);

        public Expression<Func<PassAggState, long, float, PassAggState>> Deaccumulate()
            => (oldState, timestamp, input) => oldState;

        public Expression<Func<PassAggState, PassAggState, PassAggState>> Difference()
            => (left, right) => left;

        public Expression<Func<PassAggState, float>> ComputeResult()
            => state => state.OutputStream[0];
    }
}
