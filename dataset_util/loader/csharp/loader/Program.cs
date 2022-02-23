using System;
using Stream;
using Google.Protobuf;

namespace loader
{
    class Program
    {
        static void Main(string[] args)
        {
            MessageParser<taxi_fare> parser = new MessageParser<taxi_fare>(() => new taxi_fare());
            while (true) {
                taxi_fare fare = parser.ParseDelimitedFrom(Console.OpenStandardInput());
                Console.WriteLine(fare);
            }
        }
    }
}
