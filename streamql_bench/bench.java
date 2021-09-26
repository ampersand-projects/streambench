import streamql.QL;
import streamql.algo.*;
import streamql.query.*;
import utils.structures.*;
import java.util.*;


public class bench {
    static class data{
        private long start_time;
        private long end_time;
        private float payload;

        public data(long start_time, long end_time, float payload){
            this.start_time = start_time;
            this.end_time = end_time;
            this.payload = payload;
        }
    }

    static class streamGen{
        Iterator<data> stream;
        ArrayList<data> source;

        public streamGen(long size, long period){
            source = new ArrayList<data>();
            for (int i = 0; i < size; i++){
                source.add(new data(i, i + period, (float) i));
            }
            stream = source.iterator();
        }

        public data getData(){
            if (this.stream.hasNext()){
                return stream.next();
            }
            else{
                return null;
            }   
        }  
    }

    static Algo create_test(String benchmark, long period) {
        Algo algo;
        switch(benchmark) {
            case "select":
                Q<data, data> select = QL.map(x->new data(x.start_time, x.end_time, x.payload + 3));
                algo = select.eval();
                break;
            case "where":
                Q<data, data> filter = QL.filter(x->x.payload > 0);
                algo = filter.eval();
                break;
            case "aggregate":
                Q<data, Float> aggregate = QL.tWindow(1000, QL.reduce(0f, (x,y) -> x + y.payload));
                algo = aggregate.eval();
                break;
            case "alterdur":
                Q<data, data> alterdur = QL.map(x->new data(x.start_time, x.start_time + 10 * period, x.payload));
                algo = alterdur.eval();
                break;
            case "innerjoin":
                Q<Timed<Or<data,data>>, Timed<Float>> innerjoin = QL.join((x,y)->x.payload + y.payload);
                algo = innerjoin.eval();
                break;
            default:
                System.out.println("Unknown benchmark.");
                Q<data, data> copy = QL.map(x->x);
                algo = copy.eval();
        }
        return algo;
    }

    static Sink create_sink(String benchmark) {
        Sink sink;
        switch(benchmark) {
            case "aggregate":
                sink = new Sink<Float> (){
                    @Override
                    public void next(Float item) {}
        
                    @Override
                    public void end() {
                        System.out.println("Job Done");
                    }
                };
                break;
            case "innerjoin":
                sink = new Sink<Timed<Float>> (){
                    @Override
                    public void next(Timed<Float> item) {}

                    @Override
                    public void end() {
                    System.out.println("Job Done");
                    }
                };
                break;
            case "select":
            case "where":
            case "alterdur":
            default:
                sink = new Sink<data> (){
                    @Override
                    public void next(data item) {}
        
                    @Override
                    public void end() {
                        System.out.println("Job Done");
                    }
                };
        }
        return sink;
    }

    public static void main(String[] args) {
        try{
            long size = 1000000;
            long period = 1;
            streamGen src1 = new streamGen(size, period);
            streamGen src2 = new streamGen(size, period);

            String benchmark = "innerjoin";
            String[] benchmarks = new String[] {"select", "where", "aggregate", "alterdur", "innerjoin"}; 
            if (!Arrays.asList(benchmarks).contains(benchmark)){
                throw new IllegalArgumentException();
            }
            
            var algo = create_test(benchmark, period);
            var sink = create_sink(benchmark);
            algo.connect(sink);
            algo.init();

            long startTime, endTime;
            
            if (benchmark != "innerjoin") {
                startTime = System.nanoTime();
                data curr = src1.getData();
                while (curr != null) {
                    algo.next(curr);
                    curr = src1.getData();
                };
                algo.end();
                endTime = System.nanoTime();
            }
            else {
                startTime = System.nanoTime();
                data next_left = src1.getData();
                data next_right = src2.getData();
                long curr_time = Math.min(next_left.start_time, next_right.start_time);
                long next_time;
                Or<data, data> next_item;
                while (next_left != null || next_right != null) {
                    if (next_left == null){
                        next_item = Or.right(next_right);
                        next_time = next_right.start_time;
                        next_right = src2.getData();
                    }
                    else if (next_right == null){
                        next_item = Or.left(next_left);
                        next_time = next_left.start_time;
                        next_left = src1.getData();
                    }
                    else if (next_left.start_time <= next_right.start_time){
                        next_item = Or.left(next_left);
                        next_time = next_left.start_time;
                        next_left = src1.getData();
                    }
                    else{
                        next_item = Or.right(next_right);
                        next_time = next_right.start_time;
                        next_right = src2.getData();
                    }

                    if(curr_time != next_time){
                        algo.next(new Punct(next_time - curr_time));
                        curr_time = next_time;
                    }
                    
                    algo.next(new TTLed<>(next_item, period));
                };

                algo.end();
                endTime = System.nanoTime();
            }

            System.out.println("Benchmark: " + benchmark + " Execution Time: " + (endTime-startTime));
        } catch(IllegalArgumentException e){
            System.out.println("Unknown benchmark type");
        }
    }
}
