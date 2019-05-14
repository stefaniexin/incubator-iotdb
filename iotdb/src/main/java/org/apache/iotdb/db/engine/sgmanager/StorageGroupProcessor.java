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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.Processor;
import org.apache.iotdb.db.engine.overflowdata.OverflowProcessor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.SeriesDataSource;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.IStatistic;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageGroupProcessor extends Processor implements IStatistic {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageGroupProcessor.class);

  private TsFileProcessor tsFileProcessor;
  private OverflowProcessor overflowProcessor;

  //the version controller is shared by tsfile and overflow processor.
  private VersionController versionController;

  private FileSchema fileSchema;
  /**
   * Construct processor using name space seriesPath
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public StorageGroupProcessor(String processorName)
      throws IOException, WriteProcessException, TsFileProcessorException {
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
    tsFileProcessor = new TsFileProcessor(processorName, versionController, fileSchema);
    overflowProcessor = new OverflowProcessor(processorName, versionController, fileSchema);

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

  public OperationResult insert(InsertPlan insertPlan) throws IOException, TsFileProcessorException {
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
    try {
      getTsFileProcessor().delete(device, measurementId, timestamp);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
    try {
      getOverflowProcessor().delete(device, measurementId, timestamp);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  /**
   * query data.
   */
  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws TsFileProcessorException {
    try {
      SeriesDataSource tsfileData = getTsFileProcessor().query(seriesExpression, context);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
    try {
      SeriesDataSource overflowData = getOverflowProcessor().query(seriesExpression, context);
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }

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
  public Future<Boolean> flush() {
    return null;
  }

  @Override
  public void close()  {

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

  private FileSchema constructFileSchema(String processorName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

    FileSchema schema = new FileSchema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;

  }

  public TsFileProcessor getTsFileProcessor() throws TsFileProcessorException {
    if (tsFileProcessor.isClosed()) {
      tsFileProcessor.reopen();
    }
    return tsFileProcessor;
  }

  public OverflowProcessor getOverflowProcessor() throws TsFileProcessorException {
    if (overflowProcessor.isClosed()) {
      overflowProcessor.reopen();
    }
    return overflowProcessor;
  }
}
