package edu.mit.streamjit.test.apps;

import com.google.common.collect.ImmutableList;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;
import edu.mit.streamjit.impl.interp.InterpreterStreamCompiler;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;
import edu.mit.streamjit.test.SuppliedBenchmark;
import java.util.ArrayList;
import java.util.Random;

public class MicroBench {
    private MicroBench() {
    }

    public static class Data {
        private long start_time;
        private long end_time;
        private float payload;

        public Data(long start_time, long end_time, float payload) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.payload = payload;
        }
    }

    public static ArrayList<Data> streamGen(long size, long period) {
        ArrayList<Data> source = new ArrayList<Data>();
        Random rand = new Random();
        double range = 100.0;
        for (int i = 0; i < size; i++) {
            double payload = rand.nextDouble() * range - range / 2;
            source.add(new Data(i * period, (i + 1) * period, (float) payload));
        }
        return source;
    }

    public static final class Select extends Filter<Data, Data> {
        public Select() {
            super(1, 1, 0);
        }

        @Override
        public void work() {
            Data next_item = pop();
            Data res = new Data(next_item.start_time, next_item.end_time, next_item.payload + 3);
            push(res);
        }
    }

    public static final class Where extends Filter<Data, Data> {
        public Where() {
            super(Rate.create(1), Rate.create(0, 1), Rate.create(0));
        }

        @Override
        public void work() {
            Data next_item = pop();
            if (next_item.payload > 0) {
                push(next_item);
            }
        }
    }

    public static final class Aggregate extends Filter<Data, Float> {
        int winsize;

        public Aggregate(int winsize) {
            super(winsize, 1, 0);
            this.winsize = winsize;
        }

        @Override
        public void work() {
            float sum = 0;
            for (int i = 0; i < winsize; i++) {
                sum += pop().payload;
            }
            push(sum);
        }
    }

    public static final class Alterdur extends Filter<Data, Data> {
        long period;

        public Alterdur(long period) {
            super(1, 1, 0);
            this.period = period;
        }

        @Override
        public void work() {
            Data next_item = pop();
            Data res = new Data(next_item.start_time, next_item.start_time + 10 * period, next_item.payload);
            push(res);
        }
    }

    @ServiceProvider(Benchmark.class)
    public static final class SelectBench extends SuppliedBenchmark {
        public SelectBench(ArrayList<Data> stream) {
            super("Select", Select.class, new Dataset("Datasource", Datasets.nCopies(1, Input.fromIterable(stream))));
        }
    }

    @ServiceProvider(Benchmark.class)
    public static final class WhereBench extends SuppliedBenchmark {
        public WhereBench(ArrayList<Data> stream) {
            super("Where", Where.class, new Dataset("Datasource", Datasets.nCopies(1, Input.fromIterable(stream))));
        }
    }

    @ServiceProvider(Benchmark.class)
    public static final class AggregateBench extends SuppliedBenchmark {
        public AggregateBench(ArrayList<Data> stream, int winsize) {
            super("Aggregate", Aggregate.class, ImmutableList.of(winsize),
                    new Dataset("Datasource", Datasets.nCopies(1, Input.fromIterable(stream))));
        }
    }

    @ServiceProvider(Benchmark.class)
    public static final class AlterDurBench extends SuppliedBenchmark {
        public AlterDurBench(ArrayList<Data> stream, long period) {
            super("AlterDur", Alterdur.class, ImmutableList.of(period),
                    new Dataset("Datasource", Datasets.nCopies(1, Input.fromIterable(stream))));
        }
    }

    public static void main(String[] args) {
        String benchmark = (args.length > 0) ? args[0] : "select";
        long size = (args.length > 1) ? Long.parseLong(args[1]) : 10000000;
        long period = 1;
        int winsize = 1000;
        ArrayList<Data> stream = streamGen(size, period);
        StreamCompiler sc = new DebugStreamCompiler();

        switch (benchmark) {
        case "select":
            Benchmarker.runBenchmark(new SelectBench(stream), sc).get(0).print(System.out);
            break;
        case "where":
            Benchmarker.runBenchmark(new WhereBench(stream), sc).get(0).print(System.out);
            break;
        case "aggregate":
            Benchmarker.runBenchmark(new AggregateBench(stream, winsize), sc).get(0).print(System.out);
            break;
        case "alterdur":
            Benchmarker.runBenchmark(new AlterDurBench(stream, period), sc).get(0).print(System.out);
            break;
        default:
            System.out.println("Unknown benchmark type");
        }
    }
}