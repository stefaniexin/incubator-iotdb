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
package org.apache.iotdb.db.engine.querycontext;

import java.util.List;
import org.apache.iotdb.db.engine.sgmanager.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;

public class SeriesDataSource {

  private Path seriesPath;

  // sealed tsfile
  private List<TsFileResource> sealedFiles;

  // unsealed tsfile
  private UnsealedTsFile unsealedFile;

  // seq mem-table
  private ReadOnlyMemChunk readableChunk;

  public SeriesDataSource(Path seriesPath, List<TsFileResource> sealedFiles,
      UnsealedTsFile unsealedFile,
      ReadOnlyMemChunk readableChunk) {
    this.seriesPath = seriesPath;
    this.sealedFiles = sealedFiles;
    this.unsealedFile = unsealedFile;

    this.readableChunk = readableChunk;
  }

  public boolean hasSealedTsFiles() {
    return sealedFiles != null && !sealedFiles.isEmpty();
  }

  public List<TsFileResource> getSealedFiles() {
    return sealedFiles;
  }

  public void setSealedFiles(List<TsFileResource> sealedFiles) {
    this.sealedFiles = sealedFiles;
  }

  public boolean hasUnsealedFile() {
    return unsealedFile != null;
  }

  public UnsealedTsFile getUnsealedFile() {
    return unsealedFile;
  }

  public void setUnsealedFile(UnsealedTsFile unsealedFile) {
    this.unsealedFile = unsealedFile;
  }

  public boolean hasRawSeriesChunk() {
    return readableChunk != null;
  }

  public ReadOnlyMemChunk getReadableChunk() {
    return readableChunk;
  }

  public void setReadableChunk(ReadOnlyMemChunk readableChunk) {
    this.readableChunk = readableChunk;
  }

  public Path getSeriesPath() {
    return seriesPath;
  }

  public void setSeriesPath(Path seriesPath) {
    this.seriesPath = seriesPath;
  }

}
