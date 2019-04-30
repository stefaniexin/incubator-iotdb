/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.reader.overflow;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.db.exception.TooManyChunksException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverflowSeriesSingleFileReader {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(OverflowSeriesSingleFileReader.class);
  private String device;
  private String measurement;
  private File file;

  //how many chunks can be read at the same time.
  private int numberOfWays;

  //this list should be sorted.
  private List<ConciseChunkMetadata> metadataList = new LinkedList<>();

  public OverflowSeriesSingleFileReader(String device, String measurement, File file,
      long memoryLimitInBytes) throws IOException, TooManyChunksException {
    this.device = device;
    this.measurement = measurement;
    this.file = file;
    this.numberOfWays =
        (int) memoryLimitInBytes / (TSFileConfig.pageSizeInByte + (int) ConciseChunkMetadata
            .getOccupiedMemory());

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), true)) {
      TsFileMetaData fileMetaData = reader.readFileMetadata();
      if (fileMetaData.getMeasurementSchema().containsKey(measurement)) {
        TsDeviceMetadata metadata = reader
            .readTsDeviceMetaData(fileMetaData.getDeviceMetadataIndex(device));
        for (ChunkGroupMetaData cgMetadata : metadata.getChunkGroupMetaDataList()) {
          for (ChunkMetaData metaData : cgMetadata.getChunkMetaDataList()) {
            if (metaData.getMeasurementUid().equals(measurement)) {
              this.metadataList.add(new ConciseChunkMetadata(metaData));
              if (metadataList.size() > numberOfWays) {
                throw new TooManyChunksException(new File[]{file}, device, measurement,
                    numberOfWays);
              }
              break;
            }
          }
        }
      }
      Collections.sort(this.metadataList);
    }
  }
}
