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
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class OverflowProcessor extends TsFileProcessor {

  /**
   * constructor of BufferWriteProcessor. data will be stored in baseDir/processorName/ folder.
   *
   * @param processorName processor name
   * @param fileSchemaRef file schema
   * @throws TsFileProcessorException TsFileProcessorException
   */
  public OverflowProcessor(String processorName, VersionController versionController,
      FileSchema fileSchemaRef)
      throws TsFileProcessorException, IOException {
    super(processorName, versionController,
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
}
