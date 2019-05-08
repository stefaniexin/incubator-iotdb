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

package org.apache.iotdb.db.engine.overflowdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.OverflowSeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.sgmanager.OperationResult;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class OverflowProcessor extends TsFileProcessor {

  /**
   * constructor of BufferWriteProcessor. data will be stored in baseDir/processorName/ folder.
   *
   * @param processorName processor name
   * @param fileSchemaRef file schema
   * @throws BufferWriteProcessorException BufferWriteProcessorException
   */
  public OverflowProcessor(String processorName,
      Action beforeFlushAction,
      Action afterFlushAction,
      Action afterCloseAction,
      VersionController versionController,
      FileSchema fileSchemaRef)
      throws BufferWriteProcessorException, IOException {
    super(processorName, beforeFlushAction, afterFlushAction, afterCloseAction, versionController,
        fileSchemaRef);
  }

  @Override
  protected List<String> getAllDataFolders() {
    return Directories.getInstance().getAllOverflowFileFolders();
  }

  @Override
  protected String getNextDataFolder() {
    return Directories.getInstance().getNextFolderForOverflowFile();
  }

  @Override
  protected boolean canWrite(String device, long timestamp) {
    return true;
  }

  @Override
  protected String getLogSuffix() {
    return IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX;
  }
  public OperationResult update(UpdatePlan plan) {
    return null;
  }


}
