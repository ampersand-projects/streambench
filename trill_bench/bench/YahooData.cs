using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;

namespace bench
{
    public class Interaction
    {
        public long userID;
        public long campaignID;
        public long event_type;

        public Interaction(long userID, long campaignID, long event_type)
        {
            this.userID = userID;
            this.campaignID = campaignID;
            this.event_type = event_type;
        }
    }
    public class YahooObs : TestObs<StreamEvent<Interaction>>
    {
        public YahooObs(long period, long size) : base(period, size)
        {}

        public override void Sample()
        {
            var rand = new Random();
            for (long i = 0; i < size; i++)
            {
                long userID = rand.Next(1, 5);
                long campaignID = rand.Next(1, 5);
                long event_type = rand.Next(1, 5);
                var payload = new Interaction(userID, campaignID, event_type);

                data.Add(StreamEvent.CreateInterval(i * period, (i + 1) * period, payload));
            }
        }
    }
}