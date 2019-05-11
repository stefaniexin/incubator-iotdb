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

package org.apache.iotdb.db.query.reader.overflow;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

public class ConciseChunkMetadata implements Comparable<ConciseChunkMetadata>{

  private static long EMPTY_SIZE = ObjectSizes.measure(new ConciseChunkMetadata(0,0));

  long startTime;
  /**
   * @see org.apache.iotdb.tsfile.file.metadata.ChunkMetaData#getOffsetOfChunkHeader()
   */
  long fileOffset;

  public ConciseChunkMetadata(long startTime, long fileOffset) {
    this.startTime = startTime;
    this.fileOffset = fileOffset;
  }

  public ConciseChunkMetadata(ChunkMetaData metaData) {
    this(metaData.getStartTime(), metaData.getOffsetOfChunkHeader());
  }

  public long getStartTime() {
    return startTime;
  }

  /**
   * @see org.apache.iotdb.tsfile.file.metadata.ChunkMetaData#getOffsetOfChunkHeader()
   */
  public long getFileOffset() {
    return fileOffset;
  }


  @Override
  public int compareTo(ConciseChunkMetadata o) {
    return Long.compare(startTime, o.startTime);
  }

  public static long getOccupiedMemory() {
    return EMPTY_SIZE;
  }
}
