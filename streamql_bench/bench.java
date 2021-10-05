import streamql.QL;
import streamql.algo.*;
import streamql.query.*;
import utils.structures.*;
import java.util.*;

public class Bench {
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

    public static <T> long runTest(Q<Data, T> query, ArrayList<Data> data) {
        Algo<Data, T> algo = query.eval();
        Sink<T> sink = new Sink<T>() {
            @Override
            public void next(T item) {
            }

            @Override
            public void end() {
                System.out.println("Job Done");
            }
        };
        algo.connect(sink);
        algo.init();

        long startTime = System.nanoTime();
        Data curr;
        for (int i = 0; i < data.size(); i++) {
            curr = data.get(i);
            algo.next(curr);
        }

        algo.end();
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    public static <T> long runJoinTest(Q<Timed<Or<Data, Data>>, Timed<T>> query, ArrayList<Data> data1,
            ArrayList<Data> data2) {
        Algo<Timed<Or<Data, Data>>, Timed<T>> algo = query.eval();
        Sink<Timed<T>> sink = new Sink<Timed<T>>() {
            @Override
            public void next(Timed<T> item) {
                System.out.println(item);
            }

            @Override
            public void end() {
                System.out.println("Job Done");
            }
        };
        algo.connect(sink);
        algo.init();

        long startTime = System.nanoTime();

        int i = 0, j = 0;
        long t = 0;
        while (i < data1.size() || j < data2.size()) {
            long t_left = (i < data1.size()) ? data1.get(i).start_time : Long.MAX_VALUE;
            long t_right = (j < data2.size()) ? data2.get(j).start_time : Long.MAX_VALUE;

            Or<Data, Data> next;
            Data next_left, next_right;
            long next_stime, next_etime;
            if (t_left <= t_right) {
                next_left = data1.get(i);
                next = Or.left(next_left);
                next_stime = next_left.start_time;
                next_etime = next_left.end_time;
                i++;
            } else {
                next_right = data2.get(j);
                next = Or.right(next_right);
                next_stime = next_right.start_time;
                next_etime = next_right.end_time;
                j++;
            }
            algo.next(new Punct(next_stime - t));
            algo.next(new TTLed<>(next, next_etime - next_stime));
            t = next_stime;
        }

        algo.end();
        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    public static void main(String[] args) {
        String benchmark = (args.length > 0) ? args[0] : "select";
        long size = (args.length > 1) ? Long.parseLong(args[1]) : 100000000;
        long period = 1;
        ArrayList<Data> src1 = streamGen(size, period);

        long runTime = 0;
        switch (benchmark) {
            case "select":
                Q<Data, Data> select = QL.map(x -> new Data(x.start_time, x.end_time, x.payload + 3));
                runTime = runTest(select, src1);
                break;
            case "where":
                Q<Data, Data> where = QL.filter(x -> x.payload > 0);
                runTime = runTest(where, src1);
                break;
            case "aggregate":
                Q<Data, Float> aggregate = QL.tWindow(1000, QL.reduce(0f, (x, y) -> x + y.payload));
                runTime = runTest(aggregate, src1);
                break;
            case "alterdur":
                Q<Data, Data> alterdur = QL.map(x -> new Data(x.start_time, x.start_time + 10 * period, x.payload));
                runTime = runTest(alterdur, src1);
                break;
            case "innerjoin":
                ArrayList<Data> src2 = streamGen(size, period);
                Q<Timed<Or<Data, Data>>, Timed<Float>> innerjoin = QL.join((x, y) -> x.payload + y.payload);
                runTime = runJoinTest(innerjoin, src1, src2);
                break;
            default:
                System.out.println("Unknown benchmark type");
        }

        System.out.println("Benchmark: " + benchmark + " Execution Time: " + runTime);
    }
}