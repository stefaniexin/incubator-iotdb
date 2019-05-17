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

package org.apache.iotdb.db.engine.sgmanager;

import static java.time.ZonedDateTime.ofInstant;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.overflowdata.OverflowProcessor;
import org.apache.iotdb.db.engine.pool.MergeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.SeriesDataSource;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.ImmediateFuture;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageGroupProcessor extends Processor implements IStatistic {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupProcessor.class);
  private final String statStorageGroupName;

  private TsFileProcessor tsFileProcessor;
  private OverflowProcessor overflowProcessor;

  private FileSchema fileSchema;
  private VersionController versionController;
  private long lastMergeTime;

  /**
   * oldMultiPassTokenSet records how many queries are progressing before the last merge starts.
   */
  private Set<Integer> oldMultiPassTokenSet = null;

  /**
   * newMultiPassTokenSet records how many queries are progressing since the last merge starts.
   */
  private Set<Integer> newMultiPassTokenSet = new HashSet<>();

  /**
   * newMultiPassCount assign an Id to each query to track whether this query has ended and its
   * resource can be released (whether files can be removed).
   */
  private AtomicInteger newMultiPassCount = new AtomicInteger(0);

  private boolean isMerging = false;

  /**
   * This is the modification file of the result of the current merge.
   */
  private ModificationFile mergingModification;

  private TsFileIOWriter mergeFileWriter = null;
  private String mergeOutputPath = null;
  private String mergeBaseDir = null;
  private String mergeFileName = null;
  private boolean mergeIsChunkGroupHasData = false;
  private long mergeStartPos;

  /**
   * Construct processor using StorageGroup name
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  StorageGroupProcessor(String processorName)
      throws TsFileProcessorException {
    super(processorName);

    fileSchema = constructFileSchema(processorName);

    File systemFolder = new File(IoTDBDescriptor.getInstance().getConfig().getFileNodeDir(),
        processorName);
    if (!systemFolder.exists()) {
      systemFolder.mkdirs();
      LOGGER.info("The directory of the filenode processor {} doesn't exist. Create new directory {}",
          getProcessorName(), systemFolder.getAbsolutePath());
    }
    //the version controller is shared by tsfile and overflow processor.
    try {
      versionController = new SimpleFileVersionController(systemFolder.getAbsolutePath());
    } catch (IOException e) {
      throw new TsFileProcessorException(String.format("Unable to create VersionController for %s",
          processorName), e);
    }
    tsFileProcessor = new TsFileProcessor(processorName, versionController, fileSchema);
    overflowProcessor = new OverflowProcessor(processorName, versionController, fileSchema);

    statStorageGroupName =
        MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPARATOR
            + MonitorConstants.FILE_NODE_PATH + MonitorConstants.MONITOR_PATH_SEPARATOR
            + processorName.replaceAll("\\.", "_");

    // RegisterStatService
    if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
      String statStorageDeltaName =
          MonitorConstants.STAT_STORAGE_GROUP_PREFIX + MonitorConstants.MONITOR_PATH_SEPARATOR
              + MonitorConstants.FILE_NODE_PATH + MonitorConstants.MONITOR_PATH_SEPARATOR
              + processorName.replaceAll("\\.", "_");
      StatMonitor statMonitor = StatMonitor.getInstance();
      registerStatMetadata();
      statMonitor.registerStatistics(statStorageDeltaName, this);
    }
  }

  public OperationResult insert(InsertPlan insertPlan) throws TsFileProcessorException {
    OperationResult result = getTsFileProcessor().insert(insertPlan);
    if (result == OperationResult.WRITE_REJECT_BY_TIME) {
      result = getOverflowProcessor().insert(insertPlan);
    }
    return result;
  }

  public void update(UpdatePlan plan) throws TsFileProcessorException {
    getTsFileProcessor().update(plan);
    getOverflowProcessor().update(plan);
  }

  public void delete(String device, String measurementId, long timestamp)
      throws TsFileProcessorException {
    deleteInOverflow(device, measurementId, timestamp);
    deleteInSeqFile(device, measurementId, timestamp);
    if (mergingModification != null) {
      try {
        mergingModification.write(new Deletion(device + IoTDBConstant.PATH_SEPARATOR + measurementId,
            versionController.nextVersion(), timestamp));
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws TsFileProcessorException {
    SeriesDataSource tsfileData;
    SeriesDataSource overflowData;
    try {
      tsfileData = getTsFileProcessor().query(seriesExpression, context);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
    try {
       overflowData = getOverflowProcessor().query(seriesExpression, context);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }

    return new QueryDataSource(tsfileData, overflowData);
  }

  @Override
  public boolean canBeClosed() {
    return tsFileProcessor.canBeClosed() && overflowProcessor.canBeClosed();
  }

  @Override
  public Future<Boolean> flush() throws IOException {
    Future<Boolean> tsFileFuture = tsFileProcessor.flush();
    Future<Boolean> overflowFuture = overflowProcessor.flush();

    return new StorageGroupFlushFuture(tsFileFuture, overflowFuture);
  }

  @Override
  public void close() throws TsFileProcessorException {
    tsFileProcessor.close();
    overflowProcessor.close();
  }

  @Override
  public long memoryUsage() {
    return tsFileProcessor.memoryUsage() + overflowProcessor.memoryUsage();
  }



  @Override
  public Map<String, TSRecord> getAllStatisticsValue() {
    Long curTime = System.currentTimeMillis();
    HashMap<String, TSRecord> tsRecordHashMap = new HashMap<>();
    TSRecord tsRecord = new TSRecord(curTime, statStorageGroupName);

    Map<String, AtomicLong> hashMap = getStatParamsHashMap();
    tsRecord.dataPointList = new ArrayList<>();
    for (Map.Entry<String, AtomicLong> entry : hashMap.entrySet()) {
      tsRecord.dataPointList.add(new LongDataPoint(entry.getKey(), entry.getValue().get()));
    }

    tsRecordHashMap.put(statStorageGroupName, tsRecord);
    return tsRecordHashMap;
  }

  @Override
  public void registerStatMetadata() {
    Map<String, String> hashMap = new HashMap<>();
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      hashMap
          .put(statStorageGroupName + MonitorConstants.MONITOR_PATH_SEPARATOR + statConstant.name(),
              MonitorConstants.DATA_TYPE_INT64);
    }
    StatMonitor.getInstance().registerStatStorageGroup(hashMap);
  }

  @Override
  public List<String> getAllPathForStatistic() {
    List<String> list = new ArrayList<>();
    for (MonitorConstants.FileNodeProcessorStatConstants statConstant :
        MonitorConstants.FileNodeProcessorStatConstants.values()) {
      list.add(
          statStorageGroupName + MonitorConstants.MONITOR_PATH_SEPARATOR + statConstant.name());
    }
    return list;
  }

  @Override
  public Map<String, AtomicLong> getStatParamsHashMap() {
    return null;
  }

  private FileSchema constructFileSchema(String processorName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

    FileSchema schema = new FileSchema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;

  }

  private TsFileProcessor getTsFileProcessor() throws TsFileProcessorException {
    if (tsFileProcessor.isClosed()) {
      tsFileProcessor.reopen();
    }
    return tsFileProcessor;
  }

  private OverflowProcessor getOverflowProcessor() throws TsFileProcessorException {
    if (overflowProcessor.isClosed()) {
      overflowProcessor.reopen();
    }
    return overflowProcessor;
  }

  public class StorageGroupFlushFuture implements Future<Boolean>{
    private Future<Boolean> tsFileFuture;
    private Future<Boolean> overflowFuture;

    StorageGroupFlushFuture(Future<Boolean> tsFileFuture, Future<Boolean> overflowFuture) {
      this.tsFileFuture = tsFileFuture != null ? tsFileFuture : new ImmediateFuture<>(true);
      this.overflowFuture = overflowFuture != null ? tsFileFuture : new ImmediateFuture<>(true);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean tsCanceled = tsFileFuture.cancel(mayInterruptIfRunning);
      boolean overflowCanceled = overflowFuture.cancel(mayInterruptIfRunning);
      return tsCanceled && overflowCanceled;
    }

    @Override
    public boolean isCancelled() {
      return  tsFileFuture.isCancelled() && overflowFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
      return tsFileFuture.isDone() && overflowFuture.isDone();
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
      boolean tsFileOK = tsFileFuture.get();
      boolean overflowOK = overflowFuture.get();
      return tsFileOK && overflowOK;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      boolean tsFileOK = tsFileFuture.get(timeout, unit);
      boolean overflowOK = overflowFuture.get(timeout, unit);
      return tsFileOK && overflowOK;
    }
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  long getLastInsetTime(String deviceId) throws TsFileProcessorException {
    return getTsFileProcessor().getLastInsertTime(deviceId);
  }

  void deleteInSeqFile(String deviceId, String measurementId, long timestamp)
      throws TsFileProcessorException {
    try {
      getTsFileProcessor().delete(deviceId, measurementId, timestamp);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  void deleteInOverflow(String deviceId, String measurementId, long timestamp)
      throws TsFileProcessorException {
    try {
      getOverflowProcessor().delete(deviceId, measurementId, timestamp);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  /**
   * Assign a token to a new query and put it into the newSet so that we know which
   */
  public int addMultiPassCount() {
    LOGGER.debug("Add MultiPassCount: read lock newMultiPassCount.");
    Integer token = newMultiPassCount.incrementAndGet();

    newMultiPassTokenSet.add(token);
    LOGGER.debug("Add multi token:{}, nsPath:{}.", token, getProcessorName());
    return token;
  }

  /**
   * decrease multiple pass count.
   */
  public void decreaseMultiPassCount(int token) {
    if (newMultiPassTokenSet.contains(token)) {
      newMultiPassTokenSet.remove(token);
      LOGGER.debug("Remove multi token:{}, nspath:{}, new set:{}, count:{}", token,
          getProcessorName(),
          newMultiPassTokenSet, newMultiPassCount);
    } else if (oldMultiPassTokenSet != null && oldMultiPassTokenSet.contains(token)) {
      // remove token first, then unlock
      oldMultiPassTokenSet.remove(token);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Remove multi token:{}, old set:{}", token, oldMultiPassTokenSet);
      }
    } else {
      LOGGER.error("remove token error:{},new set:{}, old set:{}", token, newMultiPassTokenSet,
          oldMultiPassTokenSet);
      // should add throw exception
    }
  }

  /**
   * append one specified TsFile to this StorageGroup.
   *
   * @param destFileResource the information of destination file
   * @param srcFilePath the path of the source file
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void appendFile(TsFileResource destFileResource, String srcFilePath)
      throws TsFileProcessorException {
    try {
      if (!destFileResource.getFile().getParentFile().exists()) {
        destFileResource.getFile().getParentFile().mkdirs();
      }
      // move file
      File srcFile = new File(srcFilePath);
      File destFile = destFileResource.getFile();
      if (!srcFile.exists()) {
        throw new FileNodeProcessorException(
            String.format("The source file %s does not exist.", srcFilePath));
      }
      if (destFile.exists()) {
        throw new FileNodeProcessorException(
            String.format("The destination file %s already exists.",
                destFileResource.getFile().getAbsolutePath()));
      }
      if (!srcFile.renameTo(destFile)) {
        LOGGER.warn("File renaming failed when appending new file. Origin: {}, Target: {}",
            srcFile.getPath(), destFile.getPath());
      }
      // append the new tsfile
      this.tsFileProcessor.appendFile(destFileResource);
      // reconstruct the inverted index of the newFileNodes
    } catch (Exception e) {
      LOGGER.error("Failed to append the tsfile {} to filenode processor {}.", destFileResource,
          getProcessorName());
      throw new TsFileProcessorException(e);
    }
  }

  /**
   * get overlap tsfiles which are conflict with the appendFile.
   *
   * @param appendFile the appended tsfile information
   */
  public List<String> getOverlapFiles(TsFileResource appendFile, String uuid)
      throws FileNodeProcessorException {
    return tsFileProcessor.getOverlapFiles(appendFile, uuid);
  }

  /**
   * register a timeseries in this StorageGroup
   * @param measurementId
   * @param dataType
   * @param encoding
   * @param compressor
   * @param props
   */
  public void addTimeSeries(String measurementId, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) {
    fileSchema.registerMeasurement(new MeasurementSchema(measurementId, dataType, encoding,
        compressor, props));
  }

  /**
   * submit the merge task to the <code>MergePool</code>.
   *
   * @return null -can't submit the merge task, because this filenode is not overflowed or it is
   * merging now. Future - submit the merge task successfully.
   */
  Future submitToMerge() {
    ZoneId zoneId = IoTDBDescriptor.getInstance().getConfig().getZoneID();
    if (lastMergeTime > 0 && LOGGER.isInfoEnabled()) {
      long thisMergeTime = System.currentTimeMillis();
      long mergeTimeInterval = thisMergeTime - lastMergeTime;
      LOGGER.info(
          "The filenode {} last merge time is {}, this merge time is {}, "
              + "merge time interval is {}s",
          processorName, ofInstant(Instant.ofEpochMilli(lastMergeTime),
              zoneId), ofInstant(Instant.ofEpochMilli(thisMergeTime),
              zoneId), mergeTimeInterval / 1000);
    }
    lastMergeTime = System.currentTimeMillis();

    if (overflowProcessor.totalFileSize() < IoTDBDescriptor.getInstance()
        .getConfig().getOverflowFileSizeThreshold()) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "Skip this merge taks submission, because the total file size {} of overflow processor {} "
                + "does not reaches the threshold {}.",
            MemUtils.bytesCntToStr(overflowProcessor.totalFileSize()), getProcessorName(),
            MemUtils.bytesCntToStr(
                IoTDBDescriptor.getInstance().getConfig().getOverflowFileSizeThreshold()));
      }
      return null;
    }

    if (!isMerging) {
      Callable<Exception> mergeThread;
      mergeThread = new MergeTask(this);
      LOGGER.info("Submit the merge task, the merge filenode is {}", processorName);
      return MergeManager.getInstance().submit(mergeThread);
    } else {
      LOGGER.warn(
          "Skip this merge task submission, because last merge task is not over yet, "
              + "the merge filenode processor is {}",
          processorName);
    }
    return null;
  }

  /**
   * Divide seqFiles and unseqFiles into k groups separately such that seqFiles in the ith group
   * overlp some unseqFiles in the ith group but overlap none unseqFiles in other groups.
   * @param seqFiles
   * @param unseqFiles
   */
  private Pair<List<List<TsFileResource>>, List<List<TsFileResource>>> groupMergeFiles(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    List<List<TsFileResource>> seqGroups = new ArrayList<>();
    List<List<TsFileResource>> unseqGroups = new ArrayList<>();
    boolean[] seqVisited = new boolean[seqFiles.size()];
    boolean[] unseqVisited = new boolean[unseqFiles.size()];
    for (int i = 0; i < seqFiles.size(); i++) {
      seqVisited[i] = false;
    }
    for (int i = 0; i < unseqFiles.size(); i++) {
      unseqVisited[i] = false;
    }

    for (int i = 0; i < seqFiles.size(); i++) {
      if (seqVisited[i]) {
        continue;
      }

      List<TsFileResource> currSeqGroup = new ArrayList<>();
      List<TsFileResource> currUnseqGroup = new ArrayList<>();

      // we only need to check new files (range [lastIndex:]) added by last iteration,
      // older files can be skipped
      int lastSeqIndex = 0;
      int lastUnseqIndex = 0;

      currSeqGroup.add(seqFiles.get(i));
      boolean updated = true;
      while (updated) {
        // find unseqFiles that overlap current seqFiles
        updated = searchOverlap(unseqFiles, unseqVisited, currSeqGroup,
            lastSeqIndex, currUnseqGroup);
        lastSeqIndex = currSeqGroup.size();
        // find seqFiles that overlap current unseqFiles
        updated = updated || searchOverlap(seqFiles, seqVisited, currUnseqGroup,
            lastUnseqIndex, currSeqGroup);
        lastUnseqIndex = currUnseqGroup.size();
      }

      if (currUnseqGroup.size() > 0) {
        seqGroups.add(currSeqGroup);
        unseqGroups.add(currUnseqGroup);
      }
    }
    return new Pair<>(seqGroups, unseqGroups);
  }

  /**
   * For each TsFile in 'candidates', ignoring the ith element if 'candidatesVisited[i]' == true,
   * find out whether it overlaps another TsFile in 'targets[offset:]' and add the candidate
   * into result.
   * @param candidates
   * @param candidatesVisited
   * @param targets
   * @param offset
   * @param result
   * @return
   */
  private boolean searchOverlap(List<TsFileResource> candidates, boolean[] candidatesVisited,
      List<TsFileResource> targets, int offset, List<TsFileResource> result) {
    boolean found = false;
    for (int j = 0; j < candidates.size(); j++) {
      if (candidatesVisited[j]) {
        continue;
      }
      TsFileResource candidate = candidates.get(j);
      for (int k = offset; k < targets.size(); k++) {
        if (candidate.overlaps(targets.get(k))) {
          result.add(candidate);
          candidatesVisited[j] = true;
          found = true;
        }
      }
    }
    return found;
  }

  /**
   * Merge this storage group, merge the tsfile data with overflow data.
   */
  public void merge() throws StorageGroupProcessorException {
    writeLock();
    Pair<List<List<TsFileResource>>, List<List<TsFileResource>>> fileGroups;
    try {
      // close seqFile and overflow(unseqFile), prepare for merge
      LOGGER.info("The StorageGroupProcessor {} begins to merge.", processorName);
      close();
      // change status from work to merge
      isMerging = true;

      // find what files are to be processed in this merge
      List<TsFileResource> seqFiles = tsFileProcessor.getTsFileResources();
      List<TsFileResource> unseqFiles = overflowProcessor.getTsFileResources();

      fileGroups = groupMergeFiles(seqFiles, unseqFiles);
    } catch (TsFileProcessorException e) {
      throw new StorageGroupProcessorException(e);
    } finally {
      writeUnlock();
      isMerging = false;
    }

    List<List<TsFileResource>> seqGroups = fileGroups.left;
    List<List<TsFileResource>> unseqGroups = fileGroups.right;

    LOGGER.info("{}: There are {} groups to be merged", processorName, seqGroups.size());

    for (int i = 0; i < seqGroups.size(); i++) {
      List<TsFileResource> seqGroup = seqGroups.get(i);
      List<TsFileResource> unseqGroup = unseqGroups.get(i);

      int seqFileSize = seqGroup.size();
      int unseqFileSize = unseqGroup.size();

      long startTime = System.currentTimeMillis();
      LOGGER.info("{}: Starting merging the {} group, there are {} seq groups and {} unseq groups",
          processorName, i, seqFileSize, unseqFileSize);
      TsFileResource newResource = mergeFileGroup(seqGroup, unseqGroup);
      long endTime = System.currentTimeMillis();
    }


    // change status from merge to wait
    switchMergeToWaiting(backupIntervalFiles, needEmtpy);

    // change status from wait to work
    switchWaitingToWorking();
  }

  private TsFileResource mergeFileGroup(List<TsFileResource> seqGroup, List<TsFileResource> unseqGroup)
      throws StorageGroupProcessorException {
    // find all timeseries (possibly) of each device (the key) contained in these files.
    Map<String, List<Path>> pathMap = null;
    try {
      pathMap = resolveMergePaths(seqGroup, unseqGroup);
    } catch (PathErrorException e) {
      throw new StorageGroupProcessorException(e);
    }
    // min data time of all files
    long minimumTime = resolveMinimumTime(seqGroup, unseqGroup);

    mergeBaseDir = Directories.getInstance().getNextFolderForTsfile();
    mergeFileName = minimumTime
        + FileNodeConstants.BUFFERWRITE_FILE_SEPARATOR + System.currentTimeMillis();
    mergeOutputPath = constructOutputFilePath(mergeBaseDir, getProcessorName(),
        mergeFileName);
    mergeFileName = getProcessorName() + File.separatorChar + mergeFileName;
    try {
      mergeFileWriter = new TsFileIOWriter(new File(mergeOutputPath));
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }
    mergingModification = new ModificationFile(mergeOutputPath
        + ModificationFile.FILE_SUFFIX);
    mergeIsChunkGroupHasData = false;
    mergeStartPos = -1;

    Map<String, Long> startTimeMap = new HashMap<>();
    Map<String, Long> endTimeMap = new HashMap<>();

    QueryContext context = new QueryContext();
    IReader seriesReader = null;
    ChunkGroupFooter footer;
    int numOfChunks = 0;
    for (Entry<String, List<Path>> entry : pathMap.entrySet()) {
      // start writing a device
      String deviceId = entry.getKey();
      List<Path> paths = entry.getValue();

      for (Path path : paths) {
        // start writing a series
        try {
          // sequence
          SeriesDataSource seqSource = new SeriesDataSource(path, seqGroup, null, null);
          SeriesDataSource unseqSource = new SeriesDataSource(path, unseqGroup, null, null);

          seriesReader = SeriesReaderFactory.getInstance().createSeriesReaderForMerge(seqSource,
              unseqSource, context);
          TSDataType dataType = MManager.getInstance().getSeriesType(path.getFullPath());
          // end writing a series
          numOfChunks += queryAndWriteSeries(seriesReader, path, null, dataType,
              startTimeMap, endTimeMap);
        } catch (PathErrorException | IOException e) {
          LOGGER.error("{}: error occurred when merging {}", processorName, path.getFullPath(), e);
        } finally {
          if (seriesReader != null) {
            try {
              seriesReader.close();
            } catch (IOException e) {
              LOGGER.error("{}: cannot close series reader of {} when merge", processorName,
                  path.getFullPath(), e);
            }
          }
        }
      }

      // end writing a device
      try {
        if (mergeIsChunkGroupHasData) {
          // end the new rowGroupMetadata
          long size = mergeFileWriter.getPos() - mergeStartPos;
          footer = new ChunkGroupFooter(deviceId, size, numOfChunks);
          mergeFileWriter.endChunkGroup(footer, 0);
        }
      } catch (IOException e) {
        LOGGER.error("{}: Cannot end ChunkGroup of {} when merge", processorName, deviceId, e);
      }

    }

    try {
      mergeFileWriter.endFile(fileSchema);
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }
    TsFileResource newResource;
    try {
      newResource = new TsFileResource(
          new File(mergeBaseDir + File.separator + mergeFileName), false);
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }
    newResource.setStartTimeMap(startTimeMap);
    newResource.setEndTimeMap(endTimeMap);
    newResource.setModFile(mergingModification);
    mergingModification = null;
    mergeFileWriter = null;
    mergeOutputPath = null;
    mergeBaseDir = null;
    mergeFileName = null;
    return newResource;
  }

  private Map<String, List<Path>> resolveMergePaths(List<TsFileResource> seqGroup, List<TsFileResource> unseqGroup)
      throws PathErrorException {
    Map<String, List<Path>> paths = new HashMap<>();

    resolveMergeGroupPaths(seqGroup, paths);
    resolveMergeGroupPaths(unseqGroup, paths);

    return paths;
  }

  private void resolveMergeGroupPaths(List<TsFileResource> fileGroup,
      Map<String, List<Path>> paths) throws PathErrorException {
    for (TsFileResource tsFile : fileGroup) {
      for (String device : tsFile.getDevices()) {
        if (paths.containsKey(device)) {
          continue;
        }
        List<String> pathStrings = MManager.getInstance().getLeafNodePathInNextLevel(device);
        for (String string : pathStrings) {
          paths.computeIfAbsent(device, x -> new ArrayList<>()).add(new Path(string));
        }
      }
    }
  }

  private long resolveMinimumTime(List<TsFileResource> seqGroup, List<TsFileResource> unseqGroup) {
    long minTime = Long.MAX_VALUE;
    for (TsFileResource tsFile : seqGroup) {
      for (Long startTime : tsFile.getStartTimeMap().values()) {
        if (startTime < minTime) {
          minTime = startTime;
        }
      }
    }
    for (TsFileResource tsFile : unseqGroup) {
      for (Long startTime : tsFile.getStartTimeMap().values()) {
        if (startTime < minTime) {
          minTime = startTime;
        }
      }
    }
    return minTime;
  }

  private String constructOutputFilePath(String baseDir, String processorName, String fileName) {

    String localBaseDir = baseDir;
    if (localBaseDir.charAt(localBaseDir.length() - 1) != File.separatorChar) {
      localBaseDir = localBaseDir + File.separatorChar + processorName;
    }
    File dataDir = new File(localBaseDir);
    if (!dataDir.exists()) {
      LOGGER.warn("The bufferwrite processor data dir doesn't exists, create new directory {}",
          localBaseDir);
      dataDir.mkdirs();
    }
    File outputFile = new File(dataDir, fileName);
    return outputFile.getPath();
  }

  private int queryAndWriteSeries(IReader seriesReader, Path path,
      SingleSeriesExpression seriesFilter, TSDataType dataType,
      Map<String, Long> startTimeMap, Map<String, Long> endTimeMap)
      throws IOException {
    int numOfChunk = 0;
    if (!seriesReader.hasNext()) {
      LOGGER.debug(
          "The time-series {} has no data with the filter {} in the filenode processor {}",
          path, seriesFilter, getProcessorName());
    } else {
      numOfChunk ++;
      TimeValuePair timeValuePair = seriesReader.next();
      if (!mergeIsChunkGroupHasData) {
        // start a new rowGroupMetadata
        mergeIsChunkGroupHasData = true;
        // the datasize and numOfChunk is fake
        // the accurate datasize and numOfChunk will get after write all this device data.
        mergeFileWriter.startFlushChunkGroup(path.getDevice());
        mergeStartPos = mergeFileWriter.getPos();
      }
      // init the serieswWriteImpl
      MeasurementSchema measurementSchema = fileSchema
          .getMeasurementSchema(path.getMeasurement());
      ChunkBuffer pageWriter = new ChunkBuffer(measurementSchema);
      int pageSizeThreshold = TSFileConfig.pageSizeInByte;
      ChunkWriterImpl seriesWriterImpl = new ChunkWriterImpl(measurementSchema, pageWriter,
          pageSizeThreshold);
      // write the series data
      writeOneSeries(path.getDevice(), seriesWriterImpl, dataType,
          seriesReader, startTimeMap, endTimeMap, timeValuePair);
      // flush the series data
      seriesWriterImpl.writeToFileWriter(mergeFileWriter);
    }
    return numOfChunk;
  }

  private void writeOneSeries(String deviceId, ChunkWriterImpl seriesWriterImpl,
      TSDataType dataType, IReader seriesReader, Map<String, Long> startTimeMap,
      Map<String, Long> endTimeMap, TimeValuePair firstTVPair) throws IOException {
    long startTime;
    long endTime;
    TimeValuePair localTV = firstTVPair;
    writeTVPair(seriesWriterImpl, dataType, localTV);
    startTime = endTime = localTV.getTimestamp();
    if (!startTimeMap.containsKey(deviceId) || startTimeMap.get(deviceId) > startTime) {
      startTimeMap.put(deviceId, startTime);
    }
    if (!endTimeMap.containsKey(deviceId) || endTimeMap.get(deviceId) < endTime) {
      endTimeMap.put(deviceId, endTime);
    }
    while (seriesReader.hasNext()) {
      localTV = seriesReader.next();
      endTime = localTV.getTimestamp();
      writeTVPair(seriesWriterImpl, dataType, localTV);
    }
    if (!endTimeMap.containsKey(deviceId) || endTimeMap.get(deviceId) < endTime) {
      endTimeMap.put(deviceId, endTime);
    }
  }

  private void writeTVPair(ChunkWriterImpl seriesWriterImpl, TSDataType dataType,
      TimeValuePair timeValuePair) throws IOException {
    switch (dataType) {
      case BOOLEAN:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT32:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case INT64:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case FLOAT:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case TEXT:
        seriesWriterImpl.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      default:
        LOGGER.error("Not support data type: {}", dataType);
        break;
    }
  }

}
