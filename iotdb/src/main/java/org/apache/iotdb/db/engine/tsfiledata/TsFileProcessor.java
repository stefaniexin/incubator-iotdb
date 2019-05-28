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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.tsfiledata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.EngingeConstants;
import org.apache.iotdb.db.engine.sgmanager.TsFileResource;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController.UsageLevel;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.MemTableFlushUtil;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.SeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.engine.sgmanager.OperationResult;
import org.apache.iotdb.db.engine.sgmanager.StorageGroupProcessor;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.sync.SyncUtils;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each storage group has a TsFileProcessor instance. Though there are many clients read/write data
 * by accessing TsFileProcessor, they need to get the readWriteReentrantLock of this processor
 * first. Therefore, as for different clients, the class looks like thread safe.
 * <br/> The class has two backend tasks:
 *  (1) submit a flush job to flush data from memtable to disk async;
 *  (2) close a tsfile and open a new one.
 * users also need to get the write lock to call these two tasks.
 */
public class TsFileProcessor extends Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileProcessor.class);

  //this is just a part of fileSchemaRef: only the measurements that belong to this TsFileProcessor
  // are in this fileSchemaRef. And, this filed is shared with other classes (i.e., storage group
  // processor), so be careful if you modify it.
  private FileSchema fileSchemaRef;


  private volatile Future<Boolean> flushFuture = new ImmediateFuture<>(true);
  //when a flush task is in the backend, then it has not conflict with write operatons (because
  // write operation modifies wokMemtable, while flush task uses flushMemtable). However, the flush
  // task has concurrent problem with query operations, because the query needs to read data from
  // flushMemtable and the table may be clear if the flush operation ends. So, a Lock is needed.
  private ReentrantLock flushQueryLock = new ReentrantLock();
  private AtomicLong memSize = new AtomicLong();

  //fileNamePrefix (system time, rather than data time) time unit: nanosecond
  // this is used for generate the new TsFile name
  private long fileNamePrefix = System.nanoTime();
  //the times of calling insertion function (between two flush operations).
  private long valueCount = 0;


  private IMemTable workMemTable;
  private IMemTable flushMemTable;
  protected RestorableTsFileIOWriter writer;
  private File insertFile;
  private TsFileResource currentResource;

  private List<TsFileResource> tsFileResources;
  // device -> datafiles
  private Map<String, List<TsFileResource>> inverseIndexOfResource;

  // device -> min timestamp in current data file
  private Map<String, Long> minWrittenTimeForEachDeviceInCurrentFile;
  // device -> max timestamp in current data file (maybe in memory)
  private Map<String, Long> maxWrittenTimeForEachDeviceInCurrentFile;
  // device -> the max timestamp in current data file on disk
  private Map<String, Long> lastFlushedTimeForEachDevice;
  //wal
  private WriteLogNode logNode;
  private VersionController versionController;

  private boolean isClosed = true;

  /**
   * constructor of BufferWriteProcessor. data will be stored in baseDir/processorName/ folder.
   *
   * @param processorName processor name
   * @param fileSchemaRef file schema
   * @throws TsFileProcessorException TsFileProcessorException
   */
  @SuppressWarnings({"squid:S2259", "squid:S3776"})
  public TsFileProcessor(String processorName, VersionController versionController,
      FileSchema fileSchemaRef) throws TsFileProcessorException {
    super(processorName);
    this.fileSchemaRef = fileSchemaRef;
    this.processorName = processorName;

    reopen();

    this.versionController = versionController;
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        logNode = MultiFileLogNodeManager.getInstance().getNode(
            processorName + getLogSuffix(),
            writer.getRestoreFilePath());
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
  }

  public void reopen() throws TsFileProcessorException {
    if (!isClosed) {
      return;
    }

    if (workMemTable == null) {
      workMemTable = new PrimitiveMemTable();
    } else {
      workMemTable.clear();
    }

    boolean noResources = tsFileResources == null;
    if (noResources) {
      initResources();
    } else {
      initCurrentTsFile(generateNewTsFilePath());
    }

    isClosed = false;
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored"})
  private void initResources() throws TsFileProcessorException {
    tsFileResources = new ArrayList<>();
    lastFlushedTimeForEachDevice = new HashMap<>();
    minWrittenTimeForEachDeviceInCurrentFile = new HashMap<>();
    maxWrittenTimeForEachDeviceInCurrentFile = new HashMap<>();
    File unclosedFile = null;
    String unclosedFileName = null;
    int unclosedFileCount = 0;
    for (String folderPath : getAllDataFolders()) {
      File dataFolder = new File(folderPath, processorName);
      if (!dataFolder.exists()) {
        // we do not add the unclosed tsfile into tsFileResources.
        File[] unclosedFiles = dataFolder
            .listFiles(x -> x.getName().contains(RestorableTsFileIOWriter.RESTORE_SUFFIX));
        if (unclosedFiles != null) {
          unclosedFileCount += unclosedFiles.length;
        }
        if (unclosedFileCount > 1) {
          break;
        } else if (unclosedFiles != null) {
          unclosedFileName = unclosedFiles[0].getName()
              .split(RestorableTsFileIOWriter.RESTORE_SUFFIX)[0];
          unclosedFile = new File(unclosedFiles[0].getParentFile(), unclosedFileName);
        }
        addResources(dataFolder, unclosedFileName);

      } else {
        //processor folder does not exist
        dataFolder.mkdirs();
      }
    }
    if (unclosedFileCount > 1) {
      throw new TsFileProcessorException(String
          .format("TsProcessor %s has more than one unclosed TsFile. please repair it",
              processorName));
    }
    if (unclosedFile == null) {
      unclosedFile = generateNewTsFilePath();
    }
    buildInverseIndex();
    initCurrentTsFile(unclosedFile);
  }

  // add TsFiles in dataFolder to tsFileResources and update device inserted time map
  private void addResources(File dataFolder, String unclosedFileName)
      throws TsFileProcessorException {
    File[] tsFiles = dataFolder
        .listFiles(x -> !x.getName().contains(RestorableTsFileIOWriter.RESTORE_SUFFIX)
            && x.getName().split(EngingeConstants.TSFILE_NAME_SEPARATOR).length == 2);
    if (tsFiles == null || tsFiles.length == 0) {
      return;
    }
    Arrays.sort(tsFiles, Comparator.comparingLong(x -> Long
        .parseLong(x.getName().split(EngingeConstants.TSFILE_NAME_SEPARATOR)[0])));

    for (File tsfile : tsFiles) {
      if (tsfile.getName().endsWith(StorageGroupProcessor.MERGE_TEMP_SUFFIX)) {
        // remove temp file of last failed merge
        //noinspection ResultOfMethodCallIgnored
        tsfile.delete();
        continue;
      }
      if (!tsfile.getName().equals(unclosedFileName)) {
        addResource(tsfile);
      }
    }
  }

  // add one TsFiles to tsFileResources and update device inserted time map
  private void addResource(File tsfile) throws TsFileProcessorException {
    //TODO we'd better define a file suffix for TsFile, e.g., .ts
    String[] names = tsfile.getName().split(EngingeConstants.TSFILE_NAME_SEPARATOR);
    long time = Long.parseLong(names[0]);
    if (fileNamePrefix < time) {
      fileNamePrefix = time;
    }
    TsFileResource resource;
    try {
      resource = new TsFileResource(tsfile, true);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
    tsFileResources.add(resource);
    //maintain the inverse index and fileNamePrefix
    for (String device : resource.getDevices()) {
      lastFlushedTimeForEachDevice
          .merge(device, resource.getEndTime(device), (x, y) -> x > y ? x : y);
    }
  }

  public void buildInverseIndex() {
    inverseIndexOfResource.clear();
    for (TsFileResource resource : tsFileResources) {
      for (String device : resource.getDevices()) {
        inverseIndexOfResource.computeIfAbsent(device, k -> new ArrayList<>()).add(resource);
      }
    }
  }


  private File generateNewTsFilePath() throws TsFileProcessorException {
    String dataDir = getNextDataFolder();
    File dataFolder = new File(dataDir, processorName);
    if (!dataFolder.exists()) {
      if (!dataFolder.mkdirs()) {
        throw new TsFileProcessorException(
            String.format("Can not create TsFileProcess related folder: %s", dataFolder));
      }
      LOGGER.debug("The bufferwrite processor data dir doesn't exists, create new directory {}.",
          dataFolder.getAbsolutePath());
    }
    String fileName = (fileNamePrefix + 1) + EngingeConstants.TSFILE_NAME_SEPARATOR
        + System.currentTimeMillis();
    return new File(dataFolder, fileName);
  }


  private void initCurrentTsFile(File file) throws TsFileProcessorException {
    this.insertFile = file;
    try {
      writer = new RestorableTsFileIOWriter(processorName, insertFile.getAbsolutePath());
      currentResource = new TsFileResource(insertFile, writer);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }

    minWrittenTimeForEachDeviceInCurrentFile.clear();
    maxWrittenTimeForEachDeviceInCurrentFile.clear();

  }

  protected boolean canWrite(String device, long timestamp) {
    return !lastFlushedTimeForEachDevice.containsKey(device)
        || timestamp > lastFlushedTimeForEachDevice.get(device);
  }
  /**
   * wrete a ts record into the memtable. If the memory usage is beyond the memThreshold, an async
   * flushing operation will be called.
   *
   * @param plan data to be written
   * @return OperationResult (WRITE_SUCCESS, WRITE_REJECT_BY_TIME, WRITE_IN_WARNING_MEM and
   * WRITE_REJECT_BY_MEM)
   * @throws TsFileProcessorException if a flushing operation occurs and failed.
   */
  public OperationResult insert(InsertPlan plan) throws TsFileProcessorException {
    if (isClosed) {
      reopen();
    }
    if (!canWrite(plan.getDeviceId(), plan.getTime())) {
      return OperationResult.WRITE_REJECT_BY_TIME;
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        logNode.write(plan);
      } catch (IOException e) {
        throw new TsFileProcessorException(String.format("Fail to write wal for %s",
            plan.toString()), e);
      }
    }
    long memUsage = 0;
    TSDataType type;
    String measurement;
    for (int i=0; i < plan.getMeasurements().length; i++){
      measurement = plan.getMeasurements()[i];
      type = fileSchemaRef.getMeasurementDataType(measurement);
      memUsage += MemUtils.getPointSize(type, measurement);
    }
    UsageLevel level = BasicMemController.getInstance().acquireUsage(this, memUsage);
    OperationResult result;
    switch (level) {
      case SAFE:
        doInsert(plan);
        checkMemThreshold4Flush(memUsage);
        result = OperationResult.WRITE_SUCCESS;
        break;
      case WARNING:
        if(LOGGER.isWarnEnabled()) {
          LOGGER.warn("Memory usage will exceed warning threshold, current : {}.",
              MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
        }
        doInsert(plan);
        try {
          flush();
        } catch (IOException e) {
          throw new TsFileProcessorException(e);
        }
        result = OperationResult.WRITE_IN_WARNING_MEM;
        break;
      case DANGEROUS:
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Memory usage will exceed dangerous threshold, current : {}.",
              MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()));
        }
        result = OperationResult.WRITE_REJECT_BY_MEM;
        break;
      default:
        result = OperationResult.WRITE_REJECT_BY_MEM;
    }
    return result;
  }

  private void doInsert(InsertPlan plan) throws TsFileProcessorException {
    writeLog(plan);
    String deviceId = plan.getDeviceId();
    long time = plan.getTime();
    TSDataType type;
    String measurement;
    for (int i = 0; i < plan.getMeasurements().length; i++) {
      measurement = plan.getMeasurements()[i];
      type = fileSchemaRef.getMeasurementDataType(measurement);
      workMemTable.write(deviceId, measurement, type, time, plan.getValues()[i]);
    }
    if (!minWrittenTimeForEachDeviceInCurrentFile.containsKey(deviceId)) {
      minWrittenTimeForEachDeviceInCurrentFile.put(deviceId, time);
    }
    if (!maxWrittenTimeForEachDeviceInCurrentFile.containsKey(deviceId)
        || maxWrittenTimeForEachDeviceInCurrentFile.get(deviceId) < time) {
      maxWrittenTimeForEachDeviceInCurrentFile.put(deviceId, time);
    }
    valueCount++;
  }

  public OperationResult update(UpdatePlan plan) {
//    String device = plan.getPath().getDevice();
//    String measurement = plan.getPath().getMeasurement();
//    List<Pair<Long, Long>> intervals = plan.getIntervals();
    //TODO modify workMemtable, flushMemtable, and existing TsFiles
    throw new UnsupportedOperationException("Update unimplemented!");
  }

  /**
   * Delete data whose timestamp <= 'timestamp' and belonging to timeseries deviceId.measurementId.
   * Delete data in both working MemTable and flushing MemTable.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the upper-bound of deletion time.
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    workMemTable.delete(deviceId, measurementId, timestamp);
    if (maxWrittenTimeForEachDeviceInCurrentFile.containsKey(deviceId)
        && maxWrittenTimeForEachDeviceInCurrentFile.get(deviceId) < timestamp) {
      maxWrittenTimeForEachDeviceInCurrentFile
          .put(deviceId, lastFlushedTimeForEachDevice.getOrDefault(deviceId, 0L));
    }
    boolean deleteFlushTable = false;
    if (isFlush()) {
      // flushing MemTable cannot be directly modified since another thread is reading it
      flushMemTable = flushMemTable.copy();
      deleteFlushTable = flushMemTable.delete(deviceId, measurementId, timestamp);
    }
    String fullPath = deviceId +
        IoTDBConstant.PATH_SEPARATOR + measurementId;
    Deletion deletion = new Deletion(fullPath, versionController.nextVersion(), timestamp);
    if (deleteFlushTable || (currentResource.containsDevice(deviceId)
        && currentResource.getStartTime(deviceId) <= timestamp)) {
      currentResource.getModFile().write(deletion);
    }
    for (TsFileResource resource : tsFileResources) {
      if (resource.containsDevice(deviceId) && resource.getStartTime(deviceId) <= timestamp) {
        resource.getModFile().write(deletion);
      }
    }
    if (lastFlushedTimeForEachDevice.containsKey(deviceId)
        && lastFlushedTimeForEachDevice.get(deviceId) <= timestamp) {
      lastFlushedTimeForEachDevice.put(deviceId, 0L);
    }
  }


  private void checkMemThreshold4Flush(long addedMemory) throws TsFileProcessorException {
    long newMem = memSize.addAndGet(addedMemory);
    if (newMem > TSFileConfig.groupSizeInByte) {
      if (LOGGER.isInfoEnabled()) {
        String usageMem = MemUtils.bytesCntToStr(newMem);
        String threshold = MemUtils.bytesCntToStr(TSFileConfig.groupSizeInByte);
        LOGGER.info("The usage of memory {} in bufferwrite processor {} reaches the threshold {}",
            usageMem, processorName, threshold);
      }
      try {
        flush();
      } catch (IOException e) {
        LOGGER.error("Flush bufferwrite error.", e);
        throw new TsFileProcessorException(e);
      }
    }
  }


  /**
   * this method is for preparing a task task and then submitting it.
   * @return
   * @throws IOException
   */
  @Override
  public Future<Boolean> flush() throws IOException {
    if (isClosed) {
      return null;
    }
    // waiting for the end of last flush operation.
    try {
      flushFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(
          "Encounter an interrupt error when waitting for the flushing, the TsFile Processor is {}.",
          getProcessorName(), e);
      Thread.currentThread().interrupt();
    }
    // statistic information for flush
    if (valueCount <= 0) {
      LOGGER.debug(
          "TsFile Processor {} has zero data to be flushed, will return directly.", processorName);
      flushFuture = new ImmediateFuture<>(true);
      return flushFuture;
    }

    if (LOGGER.isInfoEnabled()) {
      long thisFlushTime = System.currentTimeMillis();
      LOGGER.info(
          "The TsFile Processor {}: last flush time is {}, this flush time is {}, "
              + "flush time interval is {} s", getProcessorName(),
          DatetimeUtils.convertMillsecondToZonedDateTime(fileNamePrefix / 1000),
          DatetimeUtils.convertMillsecondToZonedDateTime(thisFlushTime),
          (thisFlushTime - fileNamePrefix / 1000) / 1000);
    }
    fileNamePrefix = System.nanoTime();

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      logNode.notifyStartFlush();
    }
    valueCount = 0;
    switchWorkToFlush();
    long version = versionController.nextVersion();
    BasicMemController.getInstance().releaseUsage(this, memSize.get());
    memSize.set(0);
    // switch
    flushFuture = FlushManager.getInstance().submit(() -> flushTask(version));
    return flushFuture;
  }

  /**
   * this method will be concurrent with other methods..
   *
   * @param version the operation version that will tagged on the to be flushed memtable (i.e.,
   * ChunkGroup)
   * @return true if successfully.
   */
  private boolean flushTask(long version) {
    boolean result;
    long flushStartTime = System.currentTimeMillis();
    LOGGER.info("The TsFile Processor {} starts flushing.", processorName);
    try {
      if (flushMemTable != null && !flushMemTable.isEmpty()) {
        // flush data
        MemTableFlushUtil.flushMemTable(fileSchemaRef, writer, flushMemTable,
            version);
        // write restore information
        writer.flush();
      } else {
        //no need to flush.
        return true;
      }

      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode.notifyEndFlush(null);
      }
      result = true;
    } catch (Exception e) {
      LOGGER.error(
          "The TsFile Processor {} failed to flush, when calling the afterFlushAction（filenodeFlushAction）.",
          processorName, e);
      result = false;
    } finally {
      try {
        switchFlushToWork();
      } catch (TsFileProcessorException e) {
        LOGGER.error(e.getMessage());
        result = false;
      }
      LOGGER.info("The TsFile Processor {} ends flushing.", processorName);
    }
    if (LOGGER.isInfoEnabled()) {
      long flushEndTime = System.currentTimeMillis();
      LOGGER.info(
          "The TsFile Processor {} flush, start time is {}, flush end time is {}, "
              + "flush time consumption is {}ms",
          processorName,
          DatetimeUtils.convertMillsecondToZonedDateTime(flushStartTime),
          DatetimeUtils.convertMillsecondToZonedDateTime(flushEndTime),
          flushEndTime - flushStartTime);
    }
    return result;
  }

  private void switchWorkToFlush() {
    flushQueryLock.lock();
    try {
      if (flushMemTable == null) {
        flushMemTable = workMemTable;
        for (String device : flushMemTable.getMemTableMap().keySet()) {
          lastFlushedTimeForEachDevice.put(device, maxWrittenTimeForEachDeviceInCurrentFile.get(device));
        }
        workMemTable = new PrimitiveMemTable();
      }
    } finally {
      flushQueryLock.unlock();
    }
  }

  private void switchFlushToWork() throws TsFileProcessorException {
    flushQueryLock.lock();
    try {
      //we update the index of currentTsResource.
      for (String device : flushMemTable.getMemTableMap().keySet()) {
        currentResource.setStartTime(device, minWrittenTimeForEachDeviceInCurrentFile.get(device));
        // new end time must be larger than the old one.
        currentResource.setEndTime(device, lastFlushedTimeForEachDevice.get(device));
      }
      flushMemTable.clear();
      flushMemTable = null;
      //make chunk groups in this flush task visble
      writer.appendMetadata();
      if (needCloseCurrentFile()) {
        closeCurrentTsFileAndOpenNewOne();
      }
    } finally {
      flushQueryLock.unlock();
    }
  }

  @Override
  public boolean canBeClosed() {
    return true;
  }


  /**
   * this method do not call flush() to flush data in memory to disk.
   * @throws TsFileProcessorException
   */
  private void closeCurrentTsFileAndOpenNewOne() throws TsFileProcessorException {
    closeCurrentFile();
    initCurrentTsFile(generateNewTsFilePath());
  }

  //very dangerous, how to make sure this function is thread safe (no other functions are running)
  private void closeCurrentFile() throws TsFileProcessorException {
    try {
      flush().get();
      long closeStartTime = System.currentTimeMillis();
      // end file
      if (writer.getChunkGroupMetaDatas().isEmpty()){
        //this is an empty TsFile, we do not need to save it...
        writer.endFile(fileSchemaRef);
        Files.delete(Paths.get(insertFile.getAbsolutePath()));
      } else {
        writer.endFile(fileSchemaRef);

        tsFileResources.add(currentResource);
        //maintain the inverse index
        for (String device : currentResource.getDevices()) {
          inverseIndexOfResource.computeIfAbsent(device, k -> new ArrayList<>())
              .add(currentResource);
        }
      }
      writer = null;

      // delete the restore for this bufferwrite processor
      if (LOGGER.isInfoEnabled()) {
        long closeEndTime = System.currentTimeMillis();
        LOGGER.info(
            "Close current TsFile {}, start time is {}, end time is {}, time consumption is {}ms",
            insertFile.getAbsolutePath(),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeStartTime),
            DatetimeUtils.convertMillsecondToZonedDateTime(closeEndTime),
            closeEndTime - closeStartTime);
      }

      workMemTable.clear();
      if (flushMemTable != null) {
        flushMemTable.clear();
      }
      if (logNode != null) {
        logNode.close();
      }

    } catch (IOException e) {
      LOGGER.error("Close the bufferwrite processor error, the bufferwrite is {}.",
          getProcessorName(), e);
      throw new TsFileProcessorException(e);
    } catch (Exception e) {
      LOGGER
          .error("Failed to close the bufferwrite processor when calling the action function.", e);
      throw new TsFileProcessorException(e);
    }
  }

  @Override
  public long memoryUsage() {
    return memSize.get();
  }

  /**
   * check if is flushing.
   *
   * @return True if flushing
   */
  public boolean isFlush() {
    // starting a flush task has two steps: set the flushMemtable, and then set the flushFuture
    // So, the following case exists: flushMemtable != null but flushFuture is done (because the
    // flushFuture refers to the last finished flush.
    // And, the following case exists,too: flushMemtable == null, but flushFuture is not done.
    // (flushTask() is not finished, but switchToWork() has done)
    // So, checking flushMemTable is more meaningful than flushFuture.isDone().
    return flushMemTable != null;
  }


  /**
   * query data.
   */
  public SeriesDataSource query(SingleSeriesExpression expression,
      QueryContext context) throws IOException {
    MeasurementSchema mSchema;
    TSDataType dataType;

    String deviceId = expression.getSeriesPath().getDevice();
    String measurementId = expression.getSeriesPath().getMeasurement();

    mSchema = fileSchemaRef.getMeasurementSchema(measurementId);
    dataType = mSchema.getType();

    // tsfile dataØØ
    //TODO in the old version, tsfile is deep copied. I do not know why
    List<TsFileResource> dataFiles = new ArrayList<>();
    tsFileResources.forEach(k -> {
      if (k.containsDevice(deviceId)) {
        dataFiles.add(k);
      }
    });
    // bufferwrite data
    //TODO unsealedTsFile class is a little redundant.
    UnsealedTsFile unsealedTsFile = null;

    if (currentResource.getStartTime(deviceId) >= 0) {
      unsealedTsFile = new UnsealedTsFile();
      unsealedTsFile.setFilePath(currentResource.getFile().getAbsolutePath());
      List<ChunkMetaData> chunks = writer.getMetadatas(deviceId, measurementId, dataType);
      List<Modification> pathModifications = context.getPathModifications(
          currentResource.getModFile(), deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(chunks, pathModifications);
      }

      unsealedTsFile.setTimeSeriesChunkMetaDatas(chunks);
    }
    return new SeriesDataSource(
        new Path(deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId), dataFiles,
        unsealedTsFile, queryDataInMemtable(deviceId, measurementId, dataType, mSchema.getProps()));
  }

  /**
   * get the one (or two) chunk(s) in the memtable ( and the other one in flushing status and then
   * compact them into one TimeValuePairSorter). Then get its (or their) ChunkMetadata(s).
   *
   * @param deviceId device id
   * @param measurementId sensor id
   * @param dataType data type
   * @return corresponding chunk data in memory
   */
  private ReadOnlyMemChunk queryDataInMemtable(String deviceId,
      String measurementId, TSDataType dataType, Map<String, String> props) {
    flushQueryLock.lock();
    try {
      MemSeriesLazyMerger memSeriesLazyMerger = new MemSeriesLazyMerger();
      if (flushMemTable != null) {
        memSeriesLazyMerger.addMemSeries(flushMemTable.query(deviceId, measurementId, dataType, props));
      }
      memSeriesLazyMerger.addMemSeries(workMemTable.query(deviceId, measurementId, dataType, props));
      // memSeriesLazyMerger has handled the props,
      // so we do not need to handle it again in the following readOnlyMemChunk
      return new ReadOnlyMemChunk(dataType, memSeriesLazyMerger, Collections.emptyMap());
    } finally {
      flushQueryLock.unlock();
    }
  }


  /**
   * used for test. We can know when the flush() is called.
   *
   * @return the last flush() time. Time unit: nanosecond.
   */
  public long getFileNamePrefix() {
    return fileNamePrefix;
  }

  /**
   * used for test. We can block to wait for finishing flushing.
   *
   * @return the future of the flush() task.
   */
  public Future<Boolean> getFlushFuture() {
    return flushFuture;
  }

  @Override
  public void close() throws TsFileProcessorException {
    closeCurrentFile();
    try {
      if (currentResource != null) {
        currentResource.close();
      }
      for (TsFileResource resource : tsFileResources) {
        resource.close();
      }

    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
    isClosed = true;
  }

  /**
   * remove all data of this processor. Used For UT
   */
  void removeMe() throws TsFileProcessorException, IOException {
    try {
      flushFuture.get(10000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | TimeoutException e) {
      LOGGER.error("can not end running flush task in 10 seconds: {}", e.getMessage());
    } catch (InterruptedException e) {
      LOGGER.error("current running flush task is interrupted.", e);
      Thread.currentThread().interrupt();
    }
    close();
    for (String folder : Directories.getInstance().getAllTsFileFolders()) {
      File dataFolder = new File(folder, processorName);
      File[] files;
      if (dataFolder.exists() && (files = dataFolder.listFiles()) != null) {
        for (File file: files) {
          Files.deleteIfExists(Paths.get(file.getAbsolutePath()));
        }
      }
    }
  }

  /**
   * Check if the currentTsFileResource is toooo large.
   * @return  true if the file is too large.
   */
  private boolean needCloseCurrentFile() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long fileSize = currentResource.getFile().length();
    if (fileSize >= config.getBufferwriteFileSizeThreshold()) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "The bufferwrite processor {}, size({}) of the file {} reaches threshold {}.",
            processorName, MemUtils.bytesCntToStr(fileSize), currentResource.getFilePath(),
            MemUtils.bytesCntToStr(config.getBufferwriteFileSizeThreshold()));
      }
      return true;
    }
    return false;
  }

  protected List<String> getAllDataFolders() {
    return Directories.getInstance().getAllTsFileFolders();
  }

  protected String getNextDataFolder() {
    return Directories.getInstance().getNextFolderForTsfile();
  }

  protected String getLogSuffix() {
    return IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX;
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  public boolean isClosed() {
    return isClosed;
  }

  private void writeLog(InsertPlan plan)
      throws TsFileProcessorException {
    try {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        logNode.write(plan);
      }
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public long getLastInsertTime(String deviceId) {
    // generally speaking, currentTime > historicalTime, unless currentTime does not exist
    long historicalTime = lastFlushedTimeForEachDevice.getOrDefault(deviceId, -1L);
    long currentTime = maxWrittenTimeForEachDeviceInCurrentFile.getOrDefault(deviceId, -1L);
    return Long.max(historicalTime, currentTime);
  }

  /**
   * Append a new TsFile for the sync module. The appended file can only contain new data w.r.t the
   * existing files.
   * @param tsFileResource
   */
  public void appendFile(TsFileResource tsFileResource) {
    this.tsFileResources.add(tsFileResource);
    for (Entry<String, Long> entry : tsFileResource.getEndTimeMap().entrySet()) {
      lastFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      maxWrittenTimeForEachDeviceInCurrentFile.put(entry.getKey(), entry.getValue());
      // the new file may contain new time series
      if (!minWrittenTimeForEachDeviceInCurrentFile.containsKey(entry.getKey())) {
        minWrittenTimeForEachDeviceInCurrentFile.put(entry.getKey(), entry.getValue());
      }
      inverseIndexOfResource.computeIfAbsent(entry.getKey(),
          k -> new ArrayList<>()).add(tsFileResource);
    }
  }

  /**
   * get overlap tsfiles which are conflict with the appendFile.
   *
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFiles(TsFileResource appendFile, String uuid)
      throws TsFileProcessorException {
    List<String> overlapFiles = new ArrayList<>();
    try {
      for (TsFileResource tsFileResource : tsFileResources) {
        getOverlapFile(appendFile, tsFileResource, uuid, overlapFiles);
      }
    } catch (IOException e) {
      throw new TsFileProcessorException(String.format("Failed to get tsfiles "
          + "which overlap with the appendFile: %s.", appendFile.getFilePath()), e);
    }
    return overlapFiles;
  }

  private void getOverlapFile(TsFileResource appendFile, TsFileResource tsFileResource,
      String uuid, List<String> overlapFiles) throws IOException {
    if (!tsFileResource.overlaps(appendFile)) {
      return;
    }
    // create a link to the overlapped file to avoid modifying the original file link
    File newFile = SyncUtils.linkFile(uuid, tsFileResource);
    overlapFiles.add(newFile.getPath());
  }

  /**
   * the total size of all TsFiles.
   * @return
   */
  public long totalFileSize() {
    long fileSize = 0;
    for (TsFileResource resource : tsFileResources) {
      fileSize += resource.getFile().length();
    }
    if (currentResource != null) {
      fileSize += currentResource.getFile().length();
    }
    return fileSize;
  }

  public List<TsFileResource> getTsFileResources() {
    return tsFileResources;
  }

  /**
   * Merge method, replaces 'oldFiles' in tsfileResources with 'newFile'. If 'newFile' is null, just
   * remove oldFiles.
   * @param oldFiles
   * @param newFile
   */
  public void replaceFiles(List<TsFileResource> oldFiles, TsFileResource newFile) {
    List<TsFileResource> newFiles = new ArrayList<>();
    int j = 0;
    for (TsFileResource origin : tsFileResources) {
      TsFileResource toDelete = j < oldFiles.size() ? oldFiles.get(j) : null;
      if (origin == toDelete) {
        if (j == 0 && newFile != null) {
          // replace the first old file with the new file
          newFiles.add(newFile);
        }
        // this file should be deleted, do not add it to new files
        j++;
      } else {
        // this file is not one of the new files, keep it
        newFiles.add(origin);
      }
    }
    tsFileResources = newFiles;
  }
}