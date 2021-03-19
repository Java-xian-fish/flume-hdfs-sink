package org.apache.flume.sink.hdfs;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.LinkedList;

public class ShiftController implements Runnable{

    private HDFSEventSink hdfsEventSink;
    private boolean close = false;
    private volatile LinkedList<Integer> batchCount = new LinkedList<>();
    private static final Logger LOG = LoggerFactory.getLogger(ShiftController.class);

    public ShiftController(HDFSEventSink sink){
        hdfsEventSink = sink;
    }

    public void add(Integer integer){
        batchCount.offer(integer);
    }

    @Override
    public void run() {
        LOG.info("jiang-->ShiftController is started!" );
        boolean nextDay = false;
        long count = 0l;
        while(!close){
            try {
                Date date = new Date();
                if (date.getHours()==23 && date.getMinutes()==59){
                    hdfsEventSink.setNextDay(true);
                    nextDay = true;
                    LOG.info("jiang-->ShiftController shift handle is started!" );
                }
                if (nextDay&&!hdfsEventSink.getNextDay()){
                    HDFSEventSink.parallelSink[] shiftSinks = hdfsEventSink.getShiftSink();
                    while (!HDFSEventSink.normal.isEmpty()) {
                        Event[] events = HDFSEventSink.normal.poll();
                        for (int i=0;i<events.length;i++){
                            if (events[i] != null){
                                shiftSinks[i%2].add(events[i]);
                            }
                        }
                    }
                    shiftSinks[0].close();
                    shiftSinks[1].close();
                    while (!HDFSEventSink.normalBucket.isEmpty()){
                        HDFSEventSink.normalBucket.poll().close();
                    }
                    hdfsEventSink.setCurrentDay();
                    nextDay = false;
                    LOG.info("jiang-->ShiftController shift handle is ended!" );
                }
                while (!batchCount.isEmpty()){
                    Integer i = batchCount.poll();
                    count = count + i;
                    if (count%100000000<200000&&count>=100000000){
                        long sum = 0;
                        HDFSEventSink.parallelSink[] ParallelSink = hdfsEventSink.getParallelSink();
                        for (int k=0;k<20;k++){
                            sum += ParallelSink[k].printCount();
                            LOG.info("jiang-->ShiftController-->this batch thread-->" + k + " get data-->" + ParallelSink[k].printCount() + "and the buffer size-->" + ParallelSink[k].size());
                        }
                        if (nextDay){
                            LOG.info("jiang-->ShiftController-->this batch thread-->" + 20 + " get data-->" + hdfsEventSink.getShiftSink()[0].printCount() + "and the buffer size-->" + hdfsEventSink.getShiftSink()[0].size());
                            LOG.info("jiang-->ShiftController-->this batch thread-->" + 21 + " get data-->" + hdfsEventSink.getShiftSink()[1].printCount() + "and the buffer size-->" + hdfsEventSink.getShiftSink()[1].size());
                            LOG.info("jiang-->ShiftController-->ratio-->" + hdfsEventSink.getRatio());
                        }
                        sum += hdfsEventSink.getShiftSink()[0].printCount();
                        sum += hdfsEventSink.getShiftSink()[1].printCount();
                        LOG.info("jiang-->ShiftController-->this batch has distributed-->" + i + "-->sum is-->"+ count);
                        LOG.info("jiang-->ShiftController-->The Sum Of Threads-->" + sum );
                    }
                    if (nextDay)
                        LOG.info("jiang-->ShiftController-->ratio-->" + hdfsEventSink.getRatio());
                }
            }catch (Exception e){
                LOG.error("jiang-->ShiftController" +e);
            }
        }
        LOG.info("jiang-->ShiftController is ended!" );
    }

    public void close(){
        this.close = true;
    }
}
