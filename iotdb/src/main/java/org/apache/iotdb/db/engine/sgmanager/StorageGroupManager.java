/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.sgmanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessor;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.pool.FlushManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.MonitorConstants.StorageGroupManagerStatConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StorageGroupManager provides top-level interfaces to access IoTDB storage engine. It decides
 * which StorageGroup(s) to access in order to complete a query.
 */
public class StorageGroupManager implements IStatistic, IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupManager.class);
  private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();
  private static final Directories directories = Directories.getInstance();

  /**
   * This map is used to manage all StorageGroupProcessors,<br> the key is storage group name which is
   * storage group seriesPath.
   * e.g. if the user executes a query "SET STORAGE GROUP TO root.beijing", then "root.beijing" will
   * be a key.
   */
  private ConcurrentHashMap<String, StorageGroupProcessor> processorMap;

  /**
   * storageGroupManagerStatus indicates whether the Manager is merging or being closed.
   */
  private volatile StorageGroupManagerStatus storageGroupManagerStatus = StorageGroupManagerStatus.NONE;

  private HashMap<String, AtomicLong> statParamsHashMap;

  private StorageGroupManager() {
    processorMap = new ConcurrentHashMap<>();
    initStat();
  }

  private void initStat() {
    statParamsHashMap = new HashMap<>();
    for (StorageGroupManagerStatConstants StorageGroupManagerStatConstant :
        StorageGroupManagerStatConstants.values()) {
      statParamsHashMap.put(StorageGroupManagerStatConstant.name(), new AtomicLong(0));
    }

    if (TsFileDBConf.isEnableStatMonitor()) {
      StatMonitor statMonitor = StatMonitor.getInstance();
      registerStatMetadata();
      statMonitor.registerStatistics(MonitorConstants.STAT_STORAGE_DELTA_NAME, this);
    }
  }

  public static StorageGroupManager getInstance() {
    return StorageGroupManagerHolder.INSTANCE;
  }

  private void updateStatHashMapWhenFail(TSRecord tsRecord) {
    statParamsHashMap.get(StorageGroupManagerStatConstants.TOTAL_REQ_FAIL.name())
        .incrementAndGet();
    statParamsHashMap.get(StorageGroupManagerStatConstants.TOTAL_POINTS_FAIL.name())
        .addAndGet(tsRecord.dataPointList.size());
  }

  /**
   * get stats parameter hash map.
   *
   * @return the key represents the params' name, values is AtomicLong type
   */
  @Override
  public Map<String, AtomicLong> getStatParamsHashMap() {
    return statParamsHashMap;
  }

  @Override
  public List<String> getAllPathForStatistic() {
    List<String> list = new ArrayList<>();
    for (StorageGroupManagerStatConstants statConstant :
        StorageGroupManagerStatConstants.values()) {
      list.add(MonitorConstants.STAT_STORAGE_DELTA_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
          + statConstant.name());
    }
    return list;
  }

  @Override
  public Map<String, TSRecord> getAllStatisticsValue() {
    long curTime = System.currentTimeMillis();
    TSRecord tsRecord = StatMonitor
        .convertToTSRecord(getStatParamsHashMap(), MonitorConstants.STAT_STORAGE_DELTA_NAME,
            curTime);
    HashMap<String, TSRecord> ret = new HashMap<>();
    ret.put(MonitorConstants.STAT_STORAGE_DELTA_NAME, tsRecord);
    return ret;
  }

  /**
   * Init Stat MetaDta.
   */
  @Override
  public void registerStatMetadata() {
    Map<String, String> hashMap = new HashMap<>();
    for (StorageGroupManagerStatConstants statConstant :
        StorageGroupManagerStatConstants.values()) {
      hashMap
          .put(MonitorConstants.STAT_STORAGE_DELTA_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
              + statConstant.name(), MonitorConstants.DATA_TYPE_INT64);
    }
    StatMonitor.getInstance().registerStatStorageGroup(hashMap);
  }

  /**
   * This function is just for unit test.
   */
  public synchronized void resetStorageGroupManager() {
    for (String key : statParamsHashMap.keySet()) {
      statParamsHashMap.put(key, new AtomicLong());
    }
    processorMap.clear();
  }

  /**
   * @param processorName storage name, e.g., root.a.b
   */
  private StorageGroupProcessor constructNewProcessor(String processorName)
      throws StorageGroupManagerException {
    try {
      return new StorageGroupProcessor(processorName);
    } catch (TsFileProcessorException e) {
      throw new StorageGroupManagerException(String.format("Can't construct the "
          + "StorageGroupProcessor, the StorageGroup is %s", processorName), e);
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private StorageGroupProcessor getProcessor(String path, boolean isWriteLock)
      throws StorageGroupManagerException {
    String storageGroupName;
    try {
      // return the stroage name
      storageGroupName = MManager.getInstance().getStorageGroupByPath(path);
    } catch (PathErrorException e) {
      throw new StorageGroupManagerException(
          String.format("MManager get StorageGroup name error, seriesPath is %s", path), e);
    }
    StorageGroupProcessor processor;
    processor = processorMap.get(storageGroupName);
    if (processor != null) {
      processor.lock(isWriteLock);
    } else {
      storageGroupName = storageGroupName.intern();
      // calculate the value with same key synchronously
      synchronized (storageGroupName) {
        processor = processorMap.get(storageGroupName);
        if (processor != null) {
          processor.lock(isWriteLock);
        } else {
          // calculate the value with the key monitor
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("construct a processor instance, the storage group is {}, Thread is {}",
                storageGroupName, Thread.currentThread().getId());
          }
          processor = constructNewProcessor(storageGroupName);
          processor.lock(isWriteLock);
          processorMap.put(storageGroupName, processor);
        }
      }
    }
    return processor;
  }

  /**
   * recover the StorageGroupProcessors.
   */
  public void recover() throws StorageGroupManagerException {
    List<String> storageGroupNames;
    try {
      storageGroupNames = MManager.getInstance().getAllStorageGroups();
    } catch (PathErrorException e) {
      throw new StorageGroupManagerException("Restoring all StorageGroups failed.", e);
    }
    for (String storageGroupName : storageGroupNames) {
      StorageGroupProcessor StorageGroupProcessor = null;
      try {
        StorageGroupProcessor = getProcessor(storageGroupName, true);
      } catch (StorageGroupManagerException e) {
        throw new StorageGroupManagerException(String.format("Restoring StorageGroup %s failed.",
            storageGroupName), e);
      } finally {
        if (StorageGroupProcessor != null) {
          StorageGroupProcessor.writeUnlock();
        }
      }
    }
  }

  /**
   * insert TsRecord into storage group.
   *
   * @param plan an insert plan
   * @param isMonitor if true, the insertion is done by StatMonitor and the statistic Info will not
   * be recorded. if false, the statParamsHashMap will be updated.
   */
  public void insert(InsertPlan plan, boolean isMonitor) throws StorageGroupManagerException {
    long timestamp = plan.getTime();

    String deviceId = plan.getDeviceId();
    checkTimestamp(timestamp);
    updateStat(isMonitor, plan);

    StorageGroupProcessor processor = getProcessor(deviceId, true);

    try {
      OperationResult result = processor.insert(plan);
      if (result != OperationResult.WRITE_SUCCESS) {
        throw new StorageGroupManagerException(String.format("Insertion failed due to %s",
            result.toString()));
      }
    } catch (TsFileProcessorException e) {
      throw new StorageGroupManagerException(String.format("Fail to write in SG %s",
          processor.getProcessorName()), e);
    } finally {
      processor.writeUnlock();
    }
    // Modify the insert
    if (!isMonitor) {
      processor.getStatParamsHashMap()
          .get(MonitorConstants.StorageGroupProcessorStatConstants.TOTAL_POINTS_SUCCESS.name())
          .addAndGet(plan.getValues().length);
      processor.getStatParamsHashMap()
          .get(MonitorConstants.StorageGroupProcessorStatConstants.TOTAL_REQ_SUCCESS.name())
          .incrementAndGet();
      statParamsHashMap.get(StorageGroupManagerStatConstants.TOTAL_REQ_SUCCESS.name())
          .incrementAndGet();
      statParamsHashMap
          .get(StorageGroupManagerStatConstants.TOTAL_POINTS_SUCCESS.name())
          .addAndGet(plan.getValues().length);
    }
  }

  private void checkTimestamp(long time) throws StorageGroupManagerException {
    if (time < 0) {
      throw new StorageGroupManagerException(String.format("The insert time %s is lt 0 ",
          time));
    }
  }

  private void updateStat(boolean isMonitor, InsertPlan plan) {
    if (!isMonitor) {
      statParamsHashMap.get(StorageGroupManagerStatConstants.TOTAL_POINTS.name())
          .addAndGet(plan.getValues().length);
    }
  }


  /**
   * update data.
   */
  public void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String v)
      throws StorageGroupManagerException {
    throw new UnsupportedOperationException("Method unimplemented");
  }

  /**
   * Delete data whose timestamp <= 'timestamp' of time series 'deviceId'.'measurementId'.
   * @param deviceId
   * @param measurementId
   * @param timestamp
   * @throws StorageGroupManagerException
   */
  public void deleteProcessor(String deviceId, String measurementId, long timestamp)
      throws StorageGroupManagerException {

    StorageGroupProcessor processor = getProcessor(deviceId, true);
    try {
      long lastUpdateTime;
      try {
        lastUpdateTime = processor.getLastInsetTime(deviceId);
      } catch (TsFileProcessorException e) {
        throw new StorageGroupManagerException(e);
      }
      // no TsFile data, the delete operation is invalid
      if (lastUpdateTime == -1) {
        LOGGER.debug("The last update time is -1, deletion is invalid, "
                + "the StorageGroup processor is {}",
            processor.getProcessorName());
      } else {
        try {
          processor.delete(deviceId, measurementId, timestamp);
        } catch (TsFileProcessorException e) {
          throw new StorageGroupManagerException(String.format("Fail to delete %s.%s at %d",
              deviceId, measurementId, timestamp), e);
        }
      }
    } finally {
      processor.writeUnlock();
    }
  }

  private void deleteProcessor(String processorName,
      Iterator<Map.Entry<String, StorageGroupProcessor>> processorIterator)
      throws StorageGroupManagerException {
    if (!processorMap.containsKey(processorName)) {
      LOGGER.warn("The processorMap doesn't contain the StorageGroupProcessor {}.", processorName);
      return;
    }
    LOGGER.info("Try to delete the StorageGroupProcessor {}.", processorName);
    StorageGroupProcessor processor = processorMap.get(processorName);
    if (!processor.tryWriteLock()) {
      throw new StorageGroupManagerException(String
          .format("Can't delete the StorageGroupProcessor %s because Can't get the write lock.",
              processorName));
    }

    try {
      if (!processor.canBeClosed()) {
        LOGGER.warn("The StorageGroupProcessor {} can't be deleted.", processorName);
        return;
      }

      try {
        LOGGER.info("Delete the StorageGroupProcessor {}.", processorName);
        processor.close();
        processorIterator.remove();
      } catch (TsFileProcessorException e) {
        throw new StorageGroupManagerException(String.format("Delete the StorageGroupProcessor %s"
            + " error.", processorName), e);
      }
    } finally {
      processor.writeUnlock();
    }
  }

  /**
   * Similar to delete(), but only deletes data in sequence files. Only used by WAL recovery.
   */
  public void deleteInSeqFile(String deviceId, String measurementId, long timestamp)
      throws StorageGroupManagerException {
    StorageGroupProcessor processor = getProcessor(deviceId, true);
    try {
      processor.deleteInSeqFile(deviceId, measurementId, timestamp);
    } catch (TsFileProcessorException e) {
      throw new StorageGroupManagerException(e);
    }
  }

  /**
   * Similar to delete(), but only deletes data in Overflow. Only used by WAL recovery.
   */
  public void deleteInOverflow(String deviceId, String measurementId, long timestamp)
      throws StorageGroupManagerException {
    StorageGroupProcessor processor = getProcessor(deviceId, true);
    try {
      processor.deleteInOverflow(deviceId, measurementId, timestamp);
    } catch (TsFileProcessorException e) {
      throw new StorageGroupManagerException(e);
    }
  }

  /**
   * Get a StorageGroup-level token for this query so that the StorageGroupProcessor may know which
   * queries are occupying resources.
   *
   * @param deviceId queried deviceId
   * @return a query token for the device.
   */
  public int beginQuery(String deviceId) throws StorageGroupManagerException {
    StorageGroupProcessor processor = getProcessor(deviceId, true);
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Get the StorageGroupProcessor: storage group is {}, begin query.",
            processor.getProcessorName());
      }
      return processor.addMultiPassCount();
    } finally {
      processor.writeUnlock();
    }
  }

  /**
   * Notify the storage group of 'deviceId' that query 'token' has ended and its resource can be
   * released.
   */
  public void endQuery(String deviceId, int token) throws StorageGroupManagerException {

    StorageGroupProcessor processorrocessor = getProcessor(deviceId, true);
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Get the StorageGroupProcessor: {} end query.",
            processorrocessor.getProcessorName());
      }
      processorrocessor.decreaseMultiPassCount(token);
    } finally {
      processorrocessor.writeUnlock();
    }
  }

  /**
   * Find sealed files, unsealed file and memtable data in SeqFiles and OverflowFiles that contains the
   * given series.
   * @param seriesExpression provides the path of the series.
   * @param context provides shared modifications across a query.
   * @return sealed files, unsealed file and memtable data in SeqFiles or OverflowFiles
   * @throws StorageGroupManagerException
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws StorageGroupManagerException {
    String deviceId = seriesExpression.getSeriesPath().getDevice();
    StorageGroupProcessor processor = getProcessor(deviceId, false);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Get the StorageGroupProcessor: storage group is {}, query.",
          processor.getProcessorName());
    }
    try {
      QueryDataSource queryDataSource;
      try {
        queryDataSource = processor.query(seriesExpression, context);
      } catch (TsFileProcessorException e) {
        throw new StorageGroupManagerException(e);
      }
      // return query structure
      return queryDataSource;
    } finally {
      processor.readUnlock();
    }
  }

  /**
   * Append one specified tsfile to the storage group. <b>This method is only provided for
   * transmission module</b>
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public boolean appendFileToStorageGroup(String storageGroupName, TsFileResource appendFile,
      String appendFilePath) throws StorageGroupManagerException {
    StorageGroupProcessor processor = getProcessor(storageGroupName, true);
    try {
      // check append file
      for (Map.Entry<String, Long> entry : appendFile.getStartTimeMap().entrySet()) {
        if (processor.getLastInsetTime(entry.getKey()) >= entry.getValue()) {
          return false;
        }
      }
      // close bufferwrite file
      processor.close();
      // append file to storage group.
      processor.appendFile(appendFile, appendFilePath);
    } catch (TsFileProcessorException e) {
      LOGGER.error("Cannot append the file {} to {}", appendFile.getFile().getAbsolutePath(), storageGroupName, e);
      throw new StorageGroupManagerException(e);
    } finally {
      processor.writeUnlock();
    }
    return true;
  }

  /**
   * get all overlap tsfiles which are conflict with the appendFile.
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFilesFromStorageGroup(String storageGroupName, TsFileResource appendFile,
      String uuid) throws StorageGroupManagerException {
    StorageGroupProcessor processor = getProcessor(storageGroupName, true);
    List<String> overlapFiles;
    try {
      overlapFiles = processor.getOverlapFiles(appendFile, uuid);
    } catch (TsFileProcessorException e) {
      throw new StorageGroupManagerException(e);
    } finally {
      processor.writeUnlock();
    }
    return overlapFiles;
  }

  /**
   * merge all overflowed storage group.
   *
   * @throws StorageGroupManagerException StorageGroupManagerException
   */
  public void mergeAll() throws StorageGroupManagerException {
    if (storageGroupManagerStatus != StorageGroupManagerStatus.NONE) {
      LOGGER.warn("Unable to merge all storage groups when the status is {}",
          storageGroupManagerStatus);
      return;
    }

    storageGroupManagerStatus = StorageGroupManagerStatus.MERGE;
    LOGGER.info("Start to merge all storage groups");
    List<String> storageGroupNames;
    try {
      storageGroupNames = MManager.getInstance().getAllStorageGroups();
    } catch (PathErrorException e) {
      throw new StorageGroupManagerException(e);
    }
    List<Future> futureTasks = new ArrayList<>();
    for (String storageGroupName : storageGroupNames) {
      StorageGroupProcessor processor = getProcessor(storageGroupName, true);
      try {
        Future task = processor.submitToMerge();
        if (task != null) {
          LOGGER.info("Submit the storage group {} to the merge pool", storageGroupName);
          futureTasks.add(task);
        }
      } finally {
        processor.writeUnlock();
      }
    }
    List<Exception> mergeExceptions = new ArrayList<>();
    waitForMergeTasks(futureTasks, mergeExceptions);

    if (!mergeExceptions.isEmpty()) {
      LOGGER.error("There are {} errors when merging storage groups:", mergeExceptions.size());
      for (Exception exception : mergeExceptions) {
        LOGGER.error("", exception);
      }
      throw new StorageGroupManagerException(String.format("Merge encountered %d errors,"
          + " see previous logs for details", mergeExceptions.size()));
    }
    storageGroupManagerStatus = StorageGroupManagerStatus.NONE;
    LOGGER.info("End to merge all storage groups");
  }

  private void waitForMergeTasks(List<Future> tasks, List<Exception> exceptions) {
    long totalTime = 0;
    // loop waiting for merge to end, the longest waiting time is
    // 60s.
    int time = 2;
    int singleWaitThreshold = 60;

    for (Future<?> task : tasks) {
      while (!task.isDone()) {
        try {
          LOGGER.info(
              "Waiting for the end of merge, already waiting for {}s, "
                  + "continue to wait anothor {}s",
              totalTime, time);
          TimeUnit.SECONDS.sleep(time);
          totalTime += time;
          time = updateWaitTime(time, singleWaitThreshold);
        } catch (InterruptedException e) {
          LOGGER.error("Unexpected interruption when waiting for merge", e);
          Thread.currentThread().interrupt();
        }
      }
      try {
        task.get();
      } catch (InterruptedException e) {
        LOGGER.error("Unexpected interruption when receiving merge result", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        exceptions.add(e);
      }
    }
  }

  private int updateWaitTime(int time, int threshold) {
    return time * 2 < threshold ? time * 2 : threshold;
  }

  /**
   * try to close the storage group processor. The name of storage group processor is processorName
   */
  private boolean closeOneProcessor(String processorName) throws StorageGroupManagerException {
    if (!processorMap.containsKey(processorName)) {
      return true;
    }

    StorageGroupProcessor processor = processorMap.get(processorName);
    if (processor.tryWriteLock()) {
      try {
        if (processor.canBeClosed()) {
          processor.close();
          return true;
        } else {
          return false;
        }
      } catch (TsFileProcessorException e) {
        LOGGER.error("Close the storage group processor {} error.", processorName, e);
        throw new StorageGroupManagerException(e);
      } finally {
        processor.writeUnlock();
      }
    } else {
      return false;
    }
  }

  /**
   * delete one storage group.
   */
  public void deleteOneStorageGroup(String processorName) throws StorageGroupManagerException {
    if (storageGroupManagerStatus != StorageGroupManagerStatus.NONE) {
      return;
    }

    storageGroupManagerStatus = StorageGroupManagerStatus.CLOSE;
    try {
      if (processorMap.containsKey(processorName)) {
        deleteStorageGroupBlocked(processorName);
      }

      cleanDataFiles(processorName);

      MultiFileLogNodeManager.getInstance()
          .deleteNode(processorName + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX);
      MultiFileLogNodeManager.getInstance()
          .deleteNode(processorName + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX);
    } catch (IOException e) {
      LOGGER.error("Delete the storage group processor {} error.", processorName, e);
      throw new StorageGroupManagerException(e);
    } finally {
      storageGroupManagerStatus = StorageGroupManagerStatus.NONE;
    }
  }

  private void cleanDataFiles(String processorName) throws IOException {
    List<String> seqFilePathList = directories.getAllTsFileFolders();
    List<String> overflowFilePathList = directories.getAllOverflowFileFolders();
    cleanDirectories(seqFilePathList, processorName);
    cleanDirectories(overflowFilePathList, processorName);
  }

  private void cleanDirectories(List<String> directoryPaths, String processorName) throws IOException {
    for (String directoryPath : directoryPaths) {
      directoryPath = standardizeDir(directoryPath) + processorName;
      File directory = new File(directoryPath);
      // free and close the streams under this bufferwrite directory
      if (!directory.exists()) {
        continue;
      }
      File[] dataFiles = directory.listFiles();
      if (dataFiles != null) {
        for (File dataFile : dataFiles) {
          FileReaderManager.getInstance().closeFileAndRemoveReader(dataFile.getPath());
        }
      }
      FileUtils.deleteDirectory(new File(directoryPath));
    }
  }

  private void deleteStorageGroupBlocked(String processorName) throws StorageGroupManagerException {
    LOGGER.info("Forced to delete the storage group processor {}", processorName);
    StorageGroupProcessor processor = processorMap.get(processorName);
    while (true) {
      if (processor.tryWriteLock()) {
        try {
          if (processor.canBeClosed()) {
            LOGGER.info("Delete the storage group processor {}.", processorName);
            processor.close();
            processorMap.remove(processorName);
            break;
          } else {
            LOGGER.info(
                "Can't delete the storage group  processor {}, "
                    + "because the storage group processor can't be closed."
                    + " Wait 100ms to retry");
          }
        } catch (TsFileProcessorException e) {
          throw new StorageGroupManagerException(String.
              format("Delete the storage group processor %s error.", processorName), e);
        } finally {
          processor.writeUnlock();
        }
      } else {
        LOGGER.info(
            "Can't delete the storage group processor {}, because it is already locked"
                + " Wait 100ms to retry", processorName);
      }
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.error("Unexpected interruption when waiting to delete storage group {}",
            processorName);
        Thread.currentThread().interrupt();
      }
    }
  }

  private String standardizeDir(String originalPath) {
    String res = originalPath;
    if ((originalPath.length() > 0
        && originalPath.charAt(originalPath.length() - 1) != File.separatorChar)
        || originalPath.length() == 0) {
      res = originalPath + File.separatorChar;
    }
    return res;
  }

  /**
   * add time series.
   */
  public void addTimeSeries(Path path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props) throws StorageGroupManagerException {
    StorageGroupProcessor processor = getProcessor(path.getFullPath(), true);
    try {
      processor.addTimeSeries(path.getMeasurement(), dataType, encoding, compressor, props);
    } finally {
      processor.writeUnlock();
    }
  }


  /**
   * Force to close the storage group processor.
   */
  public void closeStorageGroup(String processorName) throws StorageGroupManagerException {
    if (storageGroupManagerStatus != StorageGroupManagerStatus.NONE) {
      return;
    }

    storageGroupManagerStatus = StorageGroupManagerStatus.CLOSE;
    try {
      LOGGER.info("Force to close the storage group processor {}.", processorName);
      while (!closeOneProcessor(processorName)) {
        try {
          LOGGER.info("Can't force to close the storage group processor {}, wait 100ms to retry",
              processorName);
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          // ignore the interrupted exception
          LOGGER.error("Unexpected interruption {} when closing storage group {}",processorName,
              e);
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      storageGroupManagerStatus = StorageGroupManagerStatus.NONE;
    }
  }

  /**
   * try to close the storage group processor.
   */
  private void closeStorageGroupPrivate(String processorName) throws StorageGroupManagerException {
    if (!processorMap.containsKey(processorName)) {
      LOGGER.warn("The processorMap doesn't contain the storage group {}.", processorName);
      return;
    }
    LOGGER.info("Try to close the storage group processor {}.", processorName);
    StorageGroupProcessor processor = processorMap.get(processorName);
    if (!processor.tryWriteLock()) {
      LOGGER.warn("Can't get the write lock of the storage group processor {}.", processorName);
      return;
    }
    try {
      if (processor.canBeClosed()) {
        try {
          LOGGER.info("Close the storage group processor {}.", processorName);
          processor.close();
        } catch (TsFileProcessorException e) {
          throw new StorageGroupManagerException(String
              .format("Close the storage group processor %s error.", processorName), e);
        }
      } else {
        LOGGER.warn("The storage group processor {} can't be closed.", processorName);
      }
    } finally {
      processor.writeUnlock();
    }
  }

  /**
   * delete all storage groups.
   */
  public synchronized boolean deleteAll() throws StorageGroupManagerException {
    LOGGER.info("Start deleting all storage group");
    if (storageGroupManagerStatus != StorageGroupManagerStatus.NONE) {
      LOGGER.info("Failed to delete all storage group processor because of merge operation");
      return false;
    }

    storageGroupManagerStatus = StorageGroupManagerStatus.CLOSE;
    try {
      Iterator<Map.Entry<String, StorageGroupProcessor>> processorIterator = processorMap.entrySet()
          .iterator();
      while (processorIterator.hasNext()) {
        Map.Entry<String, StorageGroupProcessor> processorEntry = processorIterator.next();
        deleteProcessor(processorEntry.getKey(), processorIterator);
      }
      return processorMap.isEmpty();
    } finally {
      LOGGER.info("Deleting all StorageGroupProcessors ends");
      storageGroupManagerStatus = StorageGroupManagerStatus.NONE;
    }
  }

  /**
   * Try to close All.
   */
  public void closeAll() throws StorageGroupManagerException {
    LOGGER.info("Start closing all storage group processor");
    if (storageGroupManagerStatus != StorageGroupManagerStatus.NONE) {
      LOGGER.info("Failed to close all storage group processor because of merge operation");
      return;
    }
    storageGroupManagerStatus = StorageGroupManagerStatus.CLOSE;
    try {
      for (Map.Entry<String, StorageGroupProcessor> processorEntry : processorMap.entrySet()) {
        closeStorageGroupPrivate(processorEntry.getKey());
      }
    } finally {
      LOGGER.info("Close all StorageGroupProcessors ends");
      storageGroupManagerStatus = StorageGroupManagerStatus.NONE;
    }
  }

  /**
   * force flush to control memory usage.
   */
  public void forceFlush(BasicMemController.UsageLevel level) {
    switch (level) {
      // only select the most urgent (most active or biggest in size)
      // processors to flush
      // only select top 10% active memory user to flush
      case WARNING:
        try {
          flushTop(0.1f);
        } catch (IOException e) {
          LOGGER.error("force flush memory data error in waring level: ", e);
        }
        break;
      // force all processors to flush
      case DANGEROUS:
        try {
          flushAll();
        } catch (IOException e) {
          LOGGER.error("force flush memory data error in dangerous level: ", e);
        }
        break;
      case SAFE:
        break;
      default:
        throw new UnsupportedOperationException("Unreachable");
    }
  }

  private void flushAll() throws IOException {
    for (StorageGroupProcessor processor : processorMap.values()) {
      if (!processor.tryLock(true)) {
        continue;
      }
      try {
        try {
          processor.flush().get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.error("Un expected interruption when flushing storage group {}",
              processor.getProcessorName(), e);
        } catch (ExecutionException e) {
          LOGGER.error("Error occurred when flushing storage group {}",
              processor.getProcessorName(), e);
        }
      } finally {
        processor.unlock(true);
      }
    }
  }

  private void flushTop(float percentage) throws IOException {
    List<StorageGroupProcessor> tempProcessors = new ArrayList<>(processorMap.values());
    // sort the tempProcessors as descending order
    tempProcessors.sort((o1, o2) -> (int) (o2.memoryUsage() - o1.memoryUsage()));
    int flushNum =
        (int) (tempProcessors.size() * percentage) > 1
            ? (int) (tempProcessors.size() * percentage)
            : 1;
    for (int i = 0; i < flushNum && i < tempProcessors.size(); i++) {
      StorageGroupProcessor processor = tempProcessors.get(i);
      if (processor.memoryUsage() <= TSFileConfig.groupSizeInByte / 2) {
        continue;
      }
      processor.writeLock();
      try {
        try {
          processor.flush().get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.error("Unexpected interruption when flushing storage group {}",
              processor.getProcessorName(), e);
        } catch (ExecutionException e) {
          LOGGER.error("Error occurred when flushing storage group {}",
              processor.getProcessorName(), e);
        }
      } finally {
        processor.writeUnlock();
      }
    }
  }

  @Override
  public void start() {
    // do no thing
  }

  @Override
  public void stop() {
    try {
      closeAll();
    } catch (StorageGroupManagerException e) {
      LOGGER.error("Failed to close file node manager because .", e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FILE_NODE_SERVICE;
  }


  private enum StorageGroupManagerStatus {
    NONE, MERGE, CLOSE
  }

  private static class StorageGroupManagerHolder {

    private StorageGroupManagerHolder() {
    }

    private static final StorageGroupManager INSTANCE = new StorageGroupManager();
  }

}