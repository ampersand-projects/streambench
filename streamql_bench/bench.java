import streamql.QL;
import streamql.algo.*;
import streamql.query.*;
import utils.structures.*;
import java.util.*;


public class Bench {
    public static class Data{
        private long start_time;
        private long end_time;
        private float payload;

        public Data(long start_time, long end_time, float payload){
            this.start_time = start_time;
            this.end_time = end_time;
            this.payload = payload;
        }
    }

    public static ArrayList<Data> streamGen(long size, long period) {
        ArrayList<Data> source = new ArrayList<Data>();
        for (int i = 0; i < size; i++){
            source.add(new Data(i, i + period, (float) i));
        }
        return source;
    }

    public static <T> long runTest(Q<Data,T> query, ArrayList<Data> data) {
        Algo<Data,T> algo = query.eval();
        Sink<T> sink = new Sink<T> (){
            @Override
            public void next(T item) {}

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

    public static <T> long runTest(Q<Timed<Or<Data,Data>>, Timed<T>> query, ArrayList<Data> data1, ArrayList<Data> data2, long period) {
        Algo<Timed<Or<Data, Data>>, Timed<T>> algo = query.eval();
        Sink<Timed<T>> sink = new Sink<Timed<T>> (){
            @Override
            public void next(Timed<T> item) {}

            @Override
            public void end() {
            System.out.println("Job Done");
            }
        };
        algo.connect(sink);
        algo.init();

        long startTime = System.nanoTime();
        Data next_left = data1.get(0);
        Data next_right = data2.get(0);
        long curr_time = Math.min(next_left.start_time, next_right.start_time);
        long next_time;
        int i = 1, j = 1;
        Or<Data, Data> next_item;
        while (i < data1.size() || j < data2.size()) {
            if (i == data1.size()) {
                next_item = Or.right(next_right);
                next_time = next_right.start_time;
                next_right = data2.get(j++);
            }
            else if (j == data2.size()){
                next_item = Or.left(next_left);
                next_time = next_left.start_time;
                next_left = data1.get(i++);
            }
            else if (next_right.start_time <= next_left.start_time){
                next_item = Or.right(next_right);
                next_time = next_right.start_time;
                next_right = data2.get(j++);
            }
            else{
                next_item = Or.left(next_left);
                next_time = next_left.start_time;
                next_left = data1.get(i++);
            }

            if(curr_time != next_time){
                algo.next(new Punct(next_time - curr_time));
                curr_time = next_time;
            }
            
            algo.next(new TTLed<>(next_item, period));
        };

        algo.end();
        long endTime = System.nanoTime();
        return endTime - startTime;
    }


    public static void main(String[] args) {
        try{
            long size = 1000000;
            long period = 1;
            ArrayList<Data> src1 = streamGen(size, period);
            ArrayList<Data> src2 = streamGen(size, period);

            String benchmark = "innerjoin";
            String[] benchmarks = new String[] {"select", "where", "aggregate", "alterdur", "innerjoin"}; 
            if (!Arrays.asList(benchmarks).contains(benchmark)){
                throw new IllegalArgumentException();
            }
            
            long runTime = 0;
            switch(benchmark){
                case "select":
                    Q<Data, Data> select = QL.map(x->new Data(x.start_time, x.end_time, x.payload + 3));
                    runTime = runTest(select, src1);
                    break;
                case "where":
                    Q<Data, Data> where = QL.filter(x->x.payload > 0);
                    runTime = runTest(where, src1);
                    break;
                case "aggregate":
                    Q<Data, Float> aggregate = QL.tWindow(1000, QL.reduce(0f, (x,y) -> x + y.payload));
                    runTime = runTest(aggregate, src1);
                    break;
                case "alterdur":
                    Q<Data, Data> alterdur = QL.map(x->new Data(x.start_time, x.start_time + 10 * period, x.payload));
                    runTime = runTest(alterdur, src1);
                    break;
                case "innerjoin":
                    Q<Timed<Or<Data,Data>>, Timed<Float>> innerjoin = QL.join((x,y)->x.payload + y.payload);
                    runTime = runTest(innerjoin, src1, src2, period);
            }

            System.out.println("Benchmark: " + benchmark + " Execution Time: " + runTime);
        } catch(IllegalArgumentException e){
            System.out.println("Unknown benchmark type");
        }
    }
}