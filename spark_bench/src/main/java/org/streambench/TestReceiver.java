package org.streambench;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.Random;

class Event {
    public long start_time;
    public long end_time;
    public float payload;

    public Event(long start_time, long end_time, float payload) {
        this.start_time = start_time;
        this.end_time = end_time;
        this.payload = payload;
    }
}

public class TestReceiver extends Receiver<Event> {
    long period;
    long size;

    public TestReceiver(long period, long size) {
        super(StorageLevel.MEMORY_ONLY_2());
        this.period = period;
        this.size = size;
    }

    @Override
    public void onStart() {
        Random rand = new Random();
        float range = 100;
        for (long i = 0; i < size; i++) {
            float payload = rand.nextFloat() * range - (range / 2);
            store(new Event(i * period, (i + 1) * period, payload));
        }
    }

    @Override
    public void onStop() {

    }
}
