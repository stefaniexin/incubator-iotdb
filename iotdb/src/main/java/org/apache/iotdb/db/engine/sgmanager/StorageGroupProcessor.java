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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStatus;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStore;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.overflowdata.OverflowProcessor;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageGroupProcessor extends Processor implements IStatistic {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupProcessor.class);

  TsFileProcessor tsFileProcessor;
  OverflowProcessor overflowProcessor;

  //the version controller is shared by tsfile and overflow processor.
  private VersionController versionController;

  private FileSchema fileSchema;

  Action beforeFlushAction = () -> {};
  Action afterFlushAction = () -> {};
  Action afterCloseAction = () -> {};

  /**
   * Construct processor using name space seriesPath
   */
  public StorageGroupProcessor(String processorName)
      throws IOException, BufferWriteProcessorException, WriteProcessException, FileNodeProcessorException {
    super(processorName);

    this.fileSchema = constructFileSchema(processorName);

    File systemFolder = new File(IoTDBDescriptor.getInstance().getConfig().getFileNodeDir(),
        processorName);
    if (!systemFolder.exists()) {
      systemFolder.mkdirs();
      LOGGER.info("The directory of the filenode processor {} doesn't exist. Create new directory {}",
          getProcessorName(), systemFolder.getAbsolutePath());
    }
    versionController = new SimpleFileVersionController(systemFolder.getAbsolutePath());
    tsFileProcessor = new TsFileProcessor(processorName, beforeFlushAction, afterFlushAction,
        afterCloseAction, versionController, fileSchema);
    overflowProcessor = new OverflowProcessor(processorName, beforeFlushAction, afterFlushAction,
        afterCloseAction, versionController, fileSchema);

    // RegistStatService
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

  public OperationResult insert(InsertPlan insertPlan) throws IOException, BufferWriteProcessorException {
    OperationResult result = tsFileProcessor.insert(insertPlan);
    if (result == OperationResult.WRITE_REJECT_BY_TIME) {
      result = overflowProcessor.insert(insertPlan);
    }
    return result;
  }

  public void update(UpdatePlan plan) {
    tsFileProcessor.update(plan);
    overflowProcessor.update(plan);
  }

  public void delete(String device, String measurementId, long timestamp) throws IOException {
    tsFileProcessor.delete(device, measurementId, timestamp);
    overflowProcessor.delete(device, measurementId, timestamp);
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws FileNodeManagerException, IOException {
    GlobalSortedSeriesDataSource tsfileData = tsFileProcessor.query(seriesExpression, context);
    GlobalSortedSeriesDataSource overflowData = overflowProcessor.query(seriesExpression, context);
    return null;
  }


  public void addExternalTsFile() {

  }

  public void addExternalOverflowFile() {

  }

  @Override
  public boolean canBeClosed() {
    tsFileProcessor.canBeClosed();
    return false;
  }

  @Override
  public Future<Boolean> flush() throws IOException {
    return null;
  }

  @Override
  public void close() throws ProcessorException {

  }

  @Override
  public long memoryUsage() {
    return 0;
  }



  @Override
  public Map<String, TSRecord> getAllStatisticsValue() {
    return null;
  }

  @Override
  public void registerStatMetadata() {

  }

  @Override
  public List<String> getAllPathForStatistic() {
    return null;
  }

  @Override
  public Map<String, AtomicLong> getStatParamsHashMap() {
    return null;
  }

  private FileSchema constructFileSchema(String processorName) throws WriteProcessException {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

    FileSchema schema = new FileSchema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;

  }
}
