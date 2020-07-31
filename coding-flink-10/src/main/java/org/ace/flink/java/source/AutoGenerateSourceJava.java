package org.ace.flink.java.source;

import org.ace.flink.pojo.TempData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AutoGenerateSourceJava implements SourceFunction<TempData> {

    private boolean running = true;
    private String[] machines = new String[]{"machine1", "machine2", "machine3"};
    private Random rand = new Random();


    public void run(SourceContext<TempData> ctx) throws Exception {
        while(running){
            String machine = machines[rand.nextInt(machines.length)];
            System.out.println(machine);
            ctx.collect(new TempData(machine, System.currentTimeMillis(),rand.nextInt(100) ));
        }

    }

    public void cancel() {

    }

    public static void main(String[] args) {
        Random rand = new Random();
        System.out.println(rand.nextInt());
    }
 }