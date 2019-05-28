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

import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;

/**
 * StorageEngine is an abstraction of IoTDB storage-level interfaces.
 */
public interface StorageEngine {

  /**
   * This function is just for unit test.
   */
  void reset();

  /**
   * Execute an insertion.
   *
   * @param plan an insert plan
   * @param isMonitor if true, the insertion is done by StatMonitor and the statistic Info will not
   * be recorded. if false, the statParamsHashMap will be updated.
   */
  void insert(InsertPlan plan, boolean isMonitor) throws StorageGroupManagerException;

  /**
   * update data.
   */
  void update(String deviceId, String measurementId, long startTime, long endTime,
      TSDataType type, String v)
      throws StorageGroupManagerException;

  /**
   * Delete data whose timestamp <= 'timestamp' of time series 'deviceId'.'measurementId'.
   * @param deviceId
   * @param measurementId
   * @param timestamp
   * @throws StorageGroupManagerException
   */
  void deleteData(String deviceId, String measurementId, long timestamp)
          throws StorageGroupManagerException;

  /**
   * Similar to deleteData(), but only deletes data in sequence files. Only used by WAL recovery.
   */
  void deleteInSeqFile(String deviceId, String measurementId, long timestamp)
              throws StorageGroupManagerException;

  /**
   * Similar to delete(), but only deletes data in Overflow. Only used by WAL recovery.
   */
  void deleteInOverflow(String deviceId, String measurementId, long timestamp)
                  throws StorageGroupManagerException;

  /**
   * Get a StorageGroup-level token for this query so that the StorageGroupProcessor may know which
   * queries are occupying resources.
   *
   * @param deviceId queried deviceId
   * @return a query token for the device.
   */
  int beginQuery(String deviceId) throws StorageGroupManagerException;

  /**
   * Notify the storage group of 'deviceId' that query 'token' has ended and its resource can be
   * released.
   */
  void endQuery(String deviceId, int token) throws StorageGroupManagerException;

  /**
   * Find sealed files, unsealed file and memtable data in SeqFiles and OverflowFiles that contains the
   * given series.
   * @param seriesExpression provides the path of the series.
   * @param context provides shared modifications across a query.
   * @return sealed files, unsealed file and memtable data in SeqFiles or OverflowFiles
   * @throws StorageGroupManagerException
   */
  QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
      throws StorageGroupManagerException;

  /**
   * Append one specified tsfile to the storage group. <b>This method is only provided for
   * transmission module</b>
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  boolean appendFileToStorageGroup(String storageGroupName, TsFileResource appendFile,
      String appendFilePath) throws StorageGroupManagerException;

  /**
   * get all overlap tsfiles which are conflict with the appendFile.
   *
   * @param storageGroupName the seriesPath of storage group
   * @param appendFile the appended tsfile information
   */
  List<String> getOverlapFilesFromStorageGroup(String storageGroupName, TsFileResource appendFile,
      String uuid) throws StorageGroupManagerException;

  /**
   * merge all overflowed storage group.
   *
   * @throws StorageGroupManagerException StorageGroupManagerException
   */
  void mergeAll() throws StorageGroupManagerException;

  /**
   * delete one storage group.
   */
  void deleteOneStorageGroup(String processorName) throws StorageGroupManagerException;

  /**
   * add time series.
   */
  void addTimeSeries(Path path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props) throws StorageGroupManagerException;

  /**
   * Force to close the storage group processor.
   */
  void closeStorageGroup(String processorName) throws StorageGroupManagerException;

  /**
   * delete all storage groups.
   */
  boolean deleteAll() throws StorageGroupManagerException;

  /**
   * delete all storage groups.
   */
  void closeAll() throws StorageGroupManagerException;

  /**
   * force flush to control memory usage.
   */
  void forceFlush(BasicMemController.UsageLevel level);
}
