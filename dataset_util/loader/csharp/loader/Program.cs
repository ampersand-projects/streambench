using System;
using Stream;
using Google.Protobuf;

namespace loader
{
    public class DataLoader<T> where T : IMessage<T>, new()
    {
        private MessageParser<T> MsgParser = new MessageParser<T>(() => new T());

        public T LoadData()
        {
            return MsgParser.ParseDelimitedFrom(Console.OpenStandardInput());
        }
    }

    class Program
    {
        static void PrintData<T>(DataLoader<T> data_loader) where T : IMessage<T>, new()
        {
            try {
                while (true) {
                    T t = data_loader.LoadData();
                    Console.WriteLine(t);
                }
            } catch (Exception e) {
                return;
            }
        }

        static void Main(string[] args)
        {
            string dataset = "taxi_fare";
            if (args.Length > 0) {
                dataset = args[0];
            }

            if (dataset == "taxi_fare") {
                DataLoader<taxi_fare> data_loader = new DataLoader<taxi_fare>();
                PrintData<taxi_fare>(data_loader);
            } else if (dataset == "taxi_trip") {
                DataLoader<taxi_trip> data_loader = new DataLoader<taxi_trip>();
                PrintData<taxi_trip>(data_loader);
            } else {
                throw new Exception("Unknown dataset");
            }
        }
    }
}
