package org.apache.flume.sink.hdfs;

import java.util.Date;

public class ShiftController implements Runnable{

    private HDFSEventSink hdfsEventSink;
    private boolean close = false;

    public ShiftController(HDFSEventSink sink){
        hdfsEventSink = sink;
    }

    @Override
    public void run() {
        while(!close){
            try {
                Date date = new Date();
                if (date.getHours()==23 && date.getMinutes()==59){
                    hdfsEventSink.setNextDay(true);
                }

            }catch (Exception e){

            }
        }
    }

    public void close(){
        this.close = true;
    }
}
