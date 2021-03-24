/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Channel;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SystemClock;
import org.apache.flume.Transaction;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HDFSEventSink extends AbstractSink implements Configurable, BatchSizeSupported {
  public interface WriterCallback {
    public void run(String filePath);
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDFSEventSink.class);
  private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");
  private static final long defaultRollInterval = 30;
  private static final long defaultRollSize = 1024;
  private static final long defaultRollCount = 10;
  private static final String defaultFileName = "FlumeData";
  private static final String defaultSuffix = "";
  private static final String defaultInUsePrefix = "";
  private static final String defaultInUseSuffix = ".tmp";
  private static final long defaultBatchSize = 100;
  private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  private static final int defaultMaxOpenFiles = 5000;
  // Time between close retries, in seconds
  private static final long defaultRetryInterval = 180;
  // Retry forever.
  private static final int defaultTryCount = Integer.MAX_VALUE;
  public static final String IN_USE_SUFFIX_PARAM_NAME = "hdfs.inUseSuffix";
  /**
   * Default length of time we wait for blocking BucketWriter calls
   * before timing out the operation. Intended to prevent server hangs.
   */
  private static final long defaultCallTimeout = 300000;
  /**
   * Default number of threads available for tasks
   * such as append/open/close/flush with hdfs.
   * These tasks are done in a separate thread in
   * the case that they take too long. In which
   * case we create a new file and move on.
   */
  private static final int defaultThreadPoolSize = 10;
  private static final int defaultRollTimerPoolSize = 1;
  private final HDFSWriterFactory writerFactory;
  private WriterLinkedHashMap sfWriters;
  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long batchSize;
  private int threadsPoolSize;
  private int rollTimerPoolSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String fileType;
  private String filePath;
  private String fileName;
  private String suffix;
  private String inUsePrefix;
  private String inUseSuffix;
  private TimeZone timeZone;
  private int maxOpenFiles;
  private ExecutorService callTimeoutPool;
  private ScheduledExecutorService timedRollerPool;
  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;
  private long callTimeout;
  private Context context;
  private SinkCounter sinkCounter;
  private volatile int idleTimeout;
  private Clock clock;
  private FileSystem mockFs;
  private HDFSWriter mockWriter;
  private final Object sfWritersLock = new Object();
  private long retryInterval;
  private int tryCount;
  private PrivilegedExecutor privExecutor;
  private static final int THREAD_SIZE = 20;
  private volatile String day;
  private volatile parallelSink[] parallelSinks = new parallelSink[THREAD_SIZE];
  private volatile Thread[] threads = new Thread[THREAD_SIZE];
  private volatile parallelSink[] shiftSinks = new parallelSink[2];
  private volatile Thread[] shiftThreads = new Thread[3];
  private volatile ShiftController controller;
  private int ratioCount = 0;
  private int ratioSumCount = 0;
  private volatile double ratio = 1.0;
  private static final int SUM_NUM = 200000;
  private static final double FIRST_RATIO = 0.65;
  private static final double SECOND_RATIO = 0.4;
  private static final double LAST_RATIO = 0.1;
  private static final double END_RATIO = 0.005;
  private static volatile int step = 1;
  private volatile boolean nextDay = false;
  volatile static LinkedList<Event[]> normal = new LinkedList<>();
  volatile static LinkedList<BucketWriter> normalBucket = new LinkedList<>();
  private int makeSure = 0;
  /*
   * Extended Java LinkedHashMap for open file handle LRU queue.
   * We want to clear the oldest file handle if there are too many open ones.
   */

  public boolean getNextDay(){
    return this.nextDay;
  }

  public parallelSink[] getShiftSink(){
    return shiftSinks;
  }

  public void setCurrentDay(){
    this.ratio = 1.0;
    this.step = 1;
    this.ratioCount = 0;
    this.ratioSumCount = 0;
    makeSure = 0;
  }

  public void setNextDay(boolean _nextDay){
    LOG.info("jiang-->setNextDay");
    shiftSinks[0].shiftDate = true;
    shiftSinks[1].shiftDate = true;
    shiftThreads[0] = new Thread(shiftSinks[0]);
    shiftThreads[0].start();
    shiftThreads[1] = new Thread(shiftSinks[1]);
    shiftThreads[1].start();
    Date now = new Date();
    while (now.getHours()==23){
      try {
        Thread.sleep(1000);
      }catch (Exception e){
        e.printStackTrace();
      }
      now = new Date();
    }
    day = "" + now.getDate();
    this.nextDay = _nextDay;
    for (int i=0;i<THREAD_SIZE;i++){
      parallelSinks[i].shiftDate = true;
    }
    for (int i=0;i<5;i++){
      parallelSinks[i].shifting = true;
    }
    LOG.info("jiang--> end setNextDay");
  }

  private static class WriterLinkedHashMap
      extends LinkedHashMap<String, BucketWriter> {

    private final int maxOpenFiles;

    public WriterLinkedHashMap(int maxOpenFiles) {
      super(16, 0.75f, true); // stock initial capacity/load, access ordering
      this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
      if (size() > maxOpenFiles) {
        // If we have more that max open files, then close the last one and
        // return true
        try {
          eldest.getValue().close();
        } catch (InterruptedException e) {
          LOG.warn(eldest.getKey().toString(), e);
          Thread.currentThread().interrupt();
        }
        return true;
      } else {
        return false;
      }
    }
  }

  public HDFSEventSink() {
    this(new HDFSWriterFactory());
  }

  public HDFSEventSink(HDFSWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  @VisibleForTesting
  Map<String, BucketWriter> getSfWriters() {
    return sfWriters;
  }

  // read configuration and setup thresholds
  @Override
  public void configure(Context context) {
    this.context = context;
    LOG.info("HDFSEventSink---->configure(Context context)");
    filePath = Preconditions.checkNotNull(
        context.getString("hdfs.path"), "hdfs.path is required");
    fileName = context.getString("hdfs.filePrefix", defaultFileName);
    this.suffix = context.getString("hdfs.fileSuffix", defaultSuffix);
    inUsePrefix = context.getString("hdfs.inUsePrefix", defaultInUsePrefix);
    boolean emptyInUseSuffix = context.getBoolean("hdfs.emptyInUseSuffix", false);
    if (emptyInUseSuffix) {
      inUseSuffix = "";
      String tmpInUseSuffix = context.getString(IN_USE_SUFFIX_PARAM_NAME);
      if (tmpInUseSuffix != null) {
        LOG.warn("Ignoring parameter " + IN_USE_SUFFIX_PARAM_NAME + " for hdfs sink: " + getName());
      }
    } else {
      inUseSuffix = context.getString(IN_USE_SUFFIX_PARAM_NAME, defaultInUseSuffix);
    }
    String tzName = context.getString("hdfs.timeZone");
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    rollInterval = context.getLong("hdfs.rollInterval", defaultRollInterval);
    rollSize = context.getLong("hdfs.rollSize", defaultRollSize);
    rollCount = context.getLong("hdfs.rollCount", defaultRollCount);
    batchSize = context.getLong("hdfs.batchSize", defaultBatchSize);
    idleTimeout = context.getInteger("hdfs.idleTimeout", 0);
    String codecName = context.getString("hdfs.codeC");
    fileType = context.getString("hdfs.fileType", defaultFileType);
    maxOpenFiles = context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles);
    callTimeout = context.getLong("hdfs.callTimeout", defaultCallTimeout);
    threadsPoolSize = context.getInteger("hdfs.threadsPoolSize",
        defaultThreadPoolSize);
    rollTimerPoolSize = context.getInteger("hdfs.rollTimerPoolSize",
        defaultRollTimerPoolSize);
    String kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal");
    String kerbKeytab = context.getString("hdfs.kerberosKeytab");
    String proxyUser = context.getString("hdfs.proxyUser");
    tryCount = context.getInteger("hdfs.closeTries", defaultTryCount);
    if (tryCount <= 0) {
      LOG.warn("Retry count value : " + tryCount + " is not " +
          "valid. The sink will try to close the file until the file " +
          "is eventually closed.");
      tryCount = defaultTryCount;
    }
    retryInterval = context.getLong("hdfs.retryInterval", defaultRetryInterval);
    if (retryInterval <= 0) {
      LOG.warn("Retry Interval value: " + retryInterval + " is not " +
          "valid. If the first close of a file fails, " +
          "it may remain open and will not be renamed.");
      tryCount = 1;
    }

    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");
    if (codecName == null) {
      codeC = null;
      compType = CompressionType.NONE;
    } else {
      codeC = getCodec(codecName);
      // TODO : set proper compression type
      compType = CompressionType.BLOCK;
    }

    // Do not allow user to set fileType DataStream with codeC together
    // To prevent output file with compress extension (like .snappy)
    if (fileType.equalsIgnoreCase(HDFSWriterFactory.DataStreamType) && codecName != null) {
      throw new IllegalArgumentException("fileType: " + fileType +
          " which does NOT support compressed output. Please don't set codeC" +
          " or change the fileType if compressed output is desired.");
    }

    if (fileType.equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
      Preconditions.checkNotNull(codeC, "It's essential to set compress codec"
          + " when fileType is: " + fileType);
    }

    // get the appropriate executor
    this.privExecutor = FlumeAuthenticationUtil.getAuthenticator(
            kerbConfPrincipal, kerbKeytab).proxyAs(proxyUser);

    needRounding = context.getBoolean("hdfs.round", false);

    if (needRounding) {
      String unit = context.getString("hdfs.roundUnit", "second");
      if (unit.equalsIgnoreCase("hour")) {
        this.roundUnit = Calendar.HOUR_OF_DAY;
      } else if (unit.equalsIgnoreCase("minute")) {
        this.roundUnit = Calendar.MINUTE;
      } else if (unit.equalsIgnoreCase("second")) {
        this.roundUnit = Calendar.SECOND;
      } else {
        LOG.warn("Rounding unit is not valid, please set one of" +
            "minute, hour, or second. Rounding will be disabled");
        needRounding = false;
      }
      this.roundValue = context.getInteger("hdfs.roundValue", 1);
      if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
            "Round value" +
            "must be > 0 and <= 60");
      } else if (roundUnit == Calendar.HOUR_OF_DAY) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
            "Round value" +
            "must be > 0 and <= 24");
      }
    }

    this.useLocalTime = context.getBoolean("hdfs.useLocalTimeStamp", false);
    if (useLocalTime) {
      clock = new SystemClock();
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  private static boolean codecMatches(Class<? extends CompressionCodec> cls, String codecName) {
    String simpleName = cls.getSimpleName();
    if (cls.getName().equals(codecName) || simpleName.equalsIgnoreCase(codecName)) {
      return true;
    }
    if (simpleName.endsWith("Codec")) {
      String prefix = simpleName.substring(0, simpleName.length() - "Codec".length());
      if (prefix.equalsIgnoreCase(codecName)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static CompressionCodec getCodec(String codecName) {
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    CompressionCodec codec = null;
    ArrayList<String> codecStrs = new ArrayList<String>();
    codecStrs.add("None");
    for (Class<? extends CompressionCodec> cls : codecs) {
      codecStrs.add(cls.getSimpleName());
      if (codecMatches(cls, codecName)) {
        try {
          codec = cls.newInstance();
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate " + cls + " class");
        } catch (IllegalAccessException e) {
          LOG.error("Unable to access " + cls + " class");
        }
      }
    }

    if (codec == null) {
      if (!codecName.equalsIgnoreCase("None")) {
        throw new IllegalArgumentException("Unsupported compression codec "
            + codecName + ".  Please choose from: " + codecStrs);
      }
    } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
      // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
      // Must set the configuration for Configurable objects that may or do use
      // native libs
      ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
    }
    return codec;
  }

  public boolean checkIfShift(Event event){
    try {
      String data = new String(event.getBody(), 0, event.getBody().length);
      String da = data.substring(data.indexOf('-')+4).substring(0,2);
      if (!da.equals(day)) {
        return true;
      }
    }catch (Exception e){
      LOG.error(e.getLocalizedMessage());
    }
    return false;
  }

  /**
   * Pull events out of channel and send it to HDFS. Take at most batchSize
   * events per Transaction. Find the corresponding bucket for the event.
   * Ensure the file is open. Serialize the data and write it to the file on
   * HDFS. <br/>
   * This method is not thread safe.
   */
  public Status process() throws EventDeliveryException {
    //LOG.info("jiang-->HDFSEventSink---->process()");
    int batchCount = 0;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    try {
      int txnEventCount = 0,c = 0,ii = 0;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event;
        event = channel.take();
        if (event == null) {
          break;
        }
        if (!nextDay) {
          while(parallelSinks[c % THREAD_SIZE].size() >= 2000 || ii > 2000) {
            c++;
            if(c > Integer.MAX_VALUE - 60000){
              c = c - 300000000;
            }
            ii=0;
          }
          parallelSinks[c % THREAD_SIZE].add(event);
          batchCount++;
          ii++;
        }else{
          boolean checkResult = checkIfShift(event);
          ratioSumCount++;
          //只启动shift线程处理今天数据
          if(step == 1) {
            if(checkResult) {
              if(c % (THREAD_SIZE - 5 +2) >= (THREAD_SIZE -5)) {
                while(shiftSinks[c % 2].size() >= 2000 || ii > 2000) {
                  c++;
                  if(c > Integer.MAX_VALUE - 60000){
                    c = c - 300000000;
                  }
                  ii=0;
                }
                ii++;
                shiftSinks[c % 2].add(event);
                batchCount++;
              } else {
                while(parallelSinks[(c % (THREAD_SIZE -5 )) + 5].size() >= 2000 || ii > 2000) {
                  c++;
                  if(c > Integer.MAX_VALUE - 60000) {
                    c = c - 300000000;
                  }
                  ii=0;
                }
                parallelSinks[(c % (THREAD_SIZE - 5)) + 5].add(event);
                batchCount++;
                ii++;
              }
              ratioCount++;
            }else{
              while(parallelSinks[c % 5].size() >= 2000 || ii > 2000) {
                c++;
                if(c > Integer.MAX_VALUE - 60000){
                  c = c - 300000000;
                }
                ii=0;
              }
              parallelSinks[c % 5].add(event);
              batchCount++;
              ii++;
            }
          //交换shift线程和20线程交换身份
          }else if(step == 2) {
            if(checkResult){
              if(c % (THREAD_SIZE - 10 +2) >= (THREAD_SIZE -10)){
                while(shiftSinks[c % 2].size() >= 2000 || ii > 2000){
                  c++;
                  if(c > Integer.MAX_VALUE - 60000){
                    c = c - 300000000;
                  }
                  ii=0;
                }
                ii++;
                shiftSinks[c % 2].add(event);
                batchCount++;
              }else{
                while(parallelSinks[(c % (THREAD_SIZE -10 )) + 10].size() >= 2000 || ii > 2000){
                  c++;
                  if(c > Integer.MAX_VALUE - 60000){
                    c = c - 300000000;
                  }
                  ii=0;
                }
                parallelSinks[(c % (THREAD_SIZE - 10)) + 10].add(event);
                batchCount++;
                ii++;
              }
              ratioCount++;
            }else{
              while(parallelSinks[c % 10].size() >= 2000 || ii > 2000){
                c++;
                if(c > Integer.MAX_VALUE - 60000){
                  c = c - 300000000;
                }
                ii=0;
              }
              parallelSinks[c % 10].add(event);
              batchCount++;
              ii++;
            }
          //十个线程处理今天的数据
          }else if(step == 3) {
            if(checkResult){
              if(c % (THREAD_SIZE - 15 +2) >= (THREAD_SIZE -15)){
                while(shiftSinks[c % 2].size() >= 2000 || ii > 2000){
                  c++;
                  if(c > Integer.MAX_VALUE - 60000){
                    c = c - 300000000;
                  }
                  ii=0;
                }
                ii++;
                shiftSinks[c % 2].add(event);
                batchCount++;
              }else{
                while(parallelSinks[(c % (THREAD_SIZE -15 )) + 15].size() >= 2000 || ii > 2000){
                  c++;
                  if(c > Integer.MAX_VALUE - 60000){
                    c = c - 300000000;
                  }
                  ii=0;
                }
                parallelSinks[(c % (THREAD_SIZE - 15)) + 15].add(event);
                batchCount++;
                ii++;
              }
              ratioCount++;
            }else{
              while(parallelSinks[c % 15].size() >= 2000 || ii > 2000){
                c++;
                if(c > Integer.MAX_VALUE - 60000){
                  c = c - 300000000;
                }
                ii=0;
              }
              parallelSinks[c % 15].add(event);
              batchCount++;
              ii++;
            }
          //二十个线程处理当天的数据，shift线程处理漂移数据
          }else if(step == 4){
            if (checkResult) {
              if (shiftSinks[1].size()<2000){
                shiftSinks[1].add(event);
              }else if(shiftSinks[0].size()<2000){
                shiftSinks[0].add(event);
              }
              c++;
              batchCount++;
              ratioCount++;
            } else {
              while(parallelSinks[c % THREAD_SIZE].size() >= 2000 || ii > 2000){
                c++;
                if(c > Integer.MAX_VALUE - 60000){
                  c = c - 300000000;
                }
                ii=0;
              }
              parallelSinks[c % THREAD_SIZE].add(event);
              batchCount++;
              ii++;
            }
          }else{
            nextDay = false;
          }
          if (ratioSumCount==SUM_NUM){
            double _ratio = ratioCount / (double) ratioSumCount;
            if (ratio>FIRST_RATIO&&_ratio<FIRST_RATIO&&step==1) {
              if (parallelSinks[5].shiftDate){
                for (int i=5;i<10;i++){
                  parallelSinks[i].shifting = true;
                }
                LOG.info("jiang-->process FIRST_RATIO is over the 5-9 thread shift is starting!-->ratio is-->"
                        + ratio + "**_ratio is-->" + _ratio);
                ratio = _ratio >SECOND_RATIO ? _ratio : 0.6;
                step = 2;
              }
            }else if (ratio>SECOND_RATIO&&_ratio<SECOND_RATIO&&step==2) {
              if (parallelSinks[10].shiftDate){
                for (int i=10;i<15;i++){
                  parallelSinks[i].shifting = true;
                }
                LOG.info("jiang-->process SECOND_RATIO is over the 10-14 thread shift is starting!-->ratio is-->"
                        + ratio + "**_ratio is-->" + _ratio);
                ratio = _ratio > LAST_RATIO ? _ratio : 0.3;
                step = 3;
              }
            }else if (ratio>LAST_RATIO&& _ratio<LAST_RATIO&&step==3) {
              if (parallelSinks[15].shiftDate && makeSure > 10) {
                for (int i=15;i<20;i++){
                  parallelSinks[i].shifting = true;
                }
                LOG.info("jiang-->process LAST_RATIO is over the 15-19 thread shift is starting!-->ratio is-->"
                        + ratio + "**_ratio is-->" + _ratio);
                ratio = _ratio > END_RATIO ? _ratio : 0.09;
                step = 4;
              }
              makeSure++;
            }else if (ratio>END_RATIO&&_ratio<END_RATIO&&step==4) {
              if (makeSure<10) {
                makeSure++;
              }else {
                nextDay = false;
                LOG.info("jiang-->process END_RATIO is over,shift handle is over!-->ratio is-->"
                        + ratio + "**_ratio is-->" + _ratio);
                step = 5;
              }
            }else {
              ratio = _ratio;
              makeSure = 0;
            }
            ratioCount = 0;
            ratioSumCount = 0;
          }
        }
      }
      transaction.commit();
      controller.add(batchCount);
      if ( txnEventCount < 1 ) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        return Status.READY;
      }
    } catch (Exception eIO) {
      transaction.rollback();
      LOG.warn("HDFS IO error", eIO);
      sinkCounter.incrementEventWriteFail();
      return Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("process failed", th);
      sinkCounter.incrementEventWriteOrChannelFail(th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      transaction.close();
    }
  }

  @VisibleForTesting
  BucketWriter initializeBucketWriter(String realPath,
      String realName, String lookupPath, HDFSWriter hdfsWriter, WriterCallback closeCallback,int id) {
    HDFSWriter actualHdfsWriter = mockFs == null ? hdfsWriter : mockWriter;
    BucketWriter bucketWriter = new BucketWriter(rollInterval,
        rollSize, rollCount,
        batchSize, context, realPath, realName, inUsePrefix, inUseSuffix,
        suffix, codeC, compType, actualHdfsWriter, timedRollerPool,
        privExecutor, sinkCounter, idleTimeout, closeCallback,
        lookupPath, callTimeout, callTimeoutPool, retryInterval,
        tryCount);
    if (mockFs != null) {
      bucketWriter.setFileSystem(mockFs);
    }
    return bucketWriter;
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    synchronized (sfWritersLock) {
      for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
        LOG.info("Closing {}", entry.getKey());

        try {
          entry.getValue().close(false, true);
        } catch (Exception ex) {
          LOG.warn("Exception while closing " + entry.getKey() + ". " +
                  "Exception follows.", ex);
          if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    for(int i=0;i<THREAD_SIZE;i++){
      parallelSinks[i].close();
    }
    shiftSinks[0].close();
    shiftSinks[1].close();
    controller.close();
    // shut down all our thread pools
    ExecutorService[] toShutdown = { callTimeoutPool, timedRollerPool };
    for (ExecutorService execService : toShutdown) {
      execService.shutdown();
      try {
        while (execService.isTerminated() == false) {
          execService.awaitTermination(
                  Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ex) {
        LOG.warn("shutdown interrupted on " + execService, ex);
      }
    }

    callTimeoutPool = null;
    timedRollerPool = null;

    synchronized (sfWritersLock) {
      sfWriters.clear();
      sfWriters = null;
    }
    sinkCounter.stop();
    super.stop();
  }

  @Override
  public void start() {
    LOG.info("jiang-->HDFSEventSink---->start()");
    String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
    callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
            new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

    String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
    timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
            new ThreadFactoryBuilder().setNameFormat(rollerName).build());

    this.sfWriters = new WriterLinkedHashMap(maxOpenFiles);
    sinkCounter.start();
    for (int i=0;i<THREAD_SIZE;i++){
      parallelSinks[i] = new parallelSink(i);
    }
    shiftSinks[0] = new parallelSink(20);
    shiftSinks[1] = new parallelSink(21);
    for(int i=0;i<THREAD_SIZE;i++){
      threads [i] = new Thread(parallelSinks[i]);
      threads[i].start();
      LOG.info("thread" + i+ "   is started");
    }
    this.controller = new ShiftController(this);
    shiftThreads[2] = new Thread(controller);
    shiftThreads[2].start();
    super.start();
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() +
            " }";
  }

  @VisibleForTesting
  void setBucketClock(Clock clock) {
    BucketPath.setClock(clock);
  }

  @VisibleForTesting
  void setMockFs(FileSystem mockFs) {
    this.mockFs = mockFs;
  }

  public double getRatio(){
    return this.ratio;
  }

  @VisibleForTesting
  void setMockWriter(HDFSWriter writer) {
    this.mockWriter = writer;
  }

  @VisibleForTesting
  int getTryCount() {
    return tryCount;
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

  public parallelSink[] getParallelSink(){
    return parallelSinks;
  }


   class parallelSink implements Runnable {

    private final int DATA_SIZE = 2048;
    private volatile Event[] data = new Event[DATA_SIZE];
    private volatile int rear = 0;
    private volatile int front = 0;
    public volatile boolean closed = false;
    private volatile long count = 0;
    private int id;
    //标记日期应该是今天还是昨天 false：今天。true：昨天
    private volatile boolean shiftDate = false;
    private volatile boolean shifting = false;
    private boolean updateShiftDate = false;

    public boolean isShiftDate() {
      return shiftDate;
    }

    public parallelSink(int _id){
      this.id = _id;
    }

    public void close(){
      this.closed = true;
    }

    public  int  size(){
      return ( rear - front + DATA_SIZE ) % DATA_SIZE;
    }

    public void add(Event event){
      if (rear == DATA_SIZE){
        rear = 0;
      }
      data[rear++] = event;
    }

    public long printCount(){
      return count;
    }

    public void resetData(){
      Event[] _data = new Event[DATA_SIZE];
      rear = (front - 1 + DATA_SIZE) % DATA_SIZE;
      normal.offer(data);
      data = _data;
    }

    public BucketWriter getNormalBucket(String lookupPath){
      BucketWriter bucketWriter = null;
      if (!normalBucket.isEmpty()){
        synchronized (normal) {
          bucketWriter = normalBucket.poll();
          sfWriters.put(lookupPath, bucketWriter);
          bucketWriter.setShiftDate(shiftDate);
        }
        LOG.info("jiang-->run thread-->" + id + "**has handle old bucketWriter!" + bucketWriter.getBucketPath());
      }
      return bucketWriter;
    }

    @Override
    public void run() {
      int wrong = 0;
      Set<BucketWriter> writers = null;
      while ( !closed ) {
        try {
          writers = new LinkedHashSet<>();
          int txnEventCount = 0;
          while ( txnEventCount < batchSize ) {
            // reconstruct the path name by substituting place holders
            String realPath = filePath;
            String realName = "" + id;
            String lookupPath = realPath + DIRECTORY_DELIMITER + realName;
            BucketWriter bucketWriter;
            HDFSWriter hdfsWriter = null;
            // Callback to remove the reference to the bucket writer from the
            // sfWriters map so that all buffers used by the HDFS file
            // handles are garbage collected.
            WriterCallback closeCallback = new WriterCallback() {
              @Override
              public void run(String bucketPath) {
                LOG.info("Writer callback called.");
                synchronized (sfWritersLock) {
                  sfWriters.remove(bucketPath);
                }
              }
            };
            bucketWriter = sfWriters.get(lookupPath);
            if (shifting){
              synchronized (normal) {
                resetData();
                bucketWriter.setShiftDate(true);
                normalBucket.offer(bucketWriter);
                bucketWriter = null;
                rear = 0;
                front = 0;
                shiftDate = false;
              }
              shifting = false;
              updateShiftDate = false;
              LOG.info("jiang-->run thread-->" + id + "**has shifted !" );
            }
            if (shiftDate){
              if (!updateShiftDate){
                if (bucketWriter != null) {
                  bucketWriter.setShiftDate(shiftDate);
                  //closeCallback.run(lookupPath);
                  //bucketWriter.close();
                  //bucketWriter = null;
                }
                updateShiftDate = true;
              }
              if (bucketWriter == null) {
                bucketWriter = getNormalBucket(lookupPath);
              }
            }
            // we haven't seen this file yet, so open it and cache the handle
            if (bucketWriter == null) {
              hdfsWriter = writerFactory.getWriter(fileType);
              LOG.info("jiang--> hdfsWriter-->" + (hdfsWriter==null) + fileType);
              bucketWriter = initializeBucketWriter(realPath, realName, lookupPath, hdfsWriter, closeCallback, id);
              bucketWriter.setShiftDate(shiftDate);
              sfWriters.put(lookupPath, bucketWriter);
            }
            // Write the data to HDFS
            Event e = null;
            try {
              for (int iii=0;iii < 2000 && txnEventCount < batchSize;iii++) {
                while ( size() < 1 ){}
                if (front == DATA_SIZE){
                  front = 0;
                }
                e = data[front++];
                while (e == null) {
                  if (front == DATA_SIZE){
                    front = 0;
                  }
                  e = data[front++];
                }
                bucketWriter.append(e);
                txnEventCount++;
                count++;
              }
              wrong = 0;
            } catch (BucketClosedException ex) {
              LOG.info("Thread" + this.id + " Bucket was closed while trying to append, " +
                      "reinitializing bucket and writing event.");
              if (bucketWriter == null || bucketWriter.closed.get()) {
                bucketWriter = getNormalBucket(lookupPath);
              }
              hdfsWriter = writerFactory.getWriter(fileType);
              if (bucketWriter == null) {
                bucketWriter = initializeBucketWriter(realPath, realName,
                        lookupPath, hdfsWriter, closeCallback, id);
                bucketWriter.setShiftDate(shiftDate);
              }
              synchronized (sfWritersLock) {
                sfWriters.put(lookupPath, bucketWriter);
              }
              bucketWriter.append(e);
              count++;
              txnEventCount++;
            } catch (IOException ee) {
              LOG.info("jiang-->Thread" + this.id + "parallelSink-->run()-->618" + ee);
              BucketWriter _bucketWriter = sfWriters.get(lookupPath);
              wrong++;
              if ( wrong > 10 || _bucketWriter == null) {
                bucketWriter.close();
                bucketWriter = null;
              }
            }
            // track the buckets getting written in this transaction
            if (!writers.contains(bucketWriter)) {
              writers.add(bucketWriter);
            }
          }
          //LOG.info("Thread" + this.id + " endBatch");
          if (txnEventCount == 0) {
            sinkCounter.incrementBatchEmptyCount();
          } else if (txnEventCount == batchSize) {
            sinkCounter.incrementBatchCompleteCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          // flush all pending buckets before committing the transaction
          for (BucketWriter bucketWriter : writers) {
            bucketWriter.flush();
          }
        } catch (InterruptedException e) {
          LOG.info("jiang-->Thread" + this.id + " parallelSink-->run()-->639"+e);
        } catch (IOException e) {
          LOG.info("jiang-->Thread" + this.id + " parallelSink-->run()-->641"+e);
        }catch (Exception e){
          LOG.info("jiang-->Thread" + this.id + " parallelSink-->run()-->643"+e);
          e.printStackTrace();
        }
      }
      for(String bw:sfWriters.keySet()){
        try {
          sfWriters.get(bw).close();
        }catch (Exception e){
          LOG.error("jiang-->parallelSink close BucketWriter error " + e);
        }
      }
    }
  }
}
