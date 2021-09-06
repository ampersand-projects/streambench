using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;

namespace bench
{
    class Program
    {
        static void Main(string[] args)
        {
            int data_size = 100;
            var list = new List<float>();
            for (int i = 0; i < data_size; i++)
            {
                list.Add(i+1);
            }

            list
                .ToObservable()
                .ToTemporalStreamable(e => (int) e, e => (int) e + 1)
                .Select(e=>e+100)
                .ToStreamEventObservable()
                .ForEach(e => Console.WriteLine(e));
        }
    }
}
