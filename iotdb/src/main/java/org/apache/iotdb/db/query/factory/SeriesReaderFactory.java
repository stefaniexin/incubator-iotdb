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

package org.apache.iotdb.db.query.factory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.sgmanager.TsFileResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.SeriesDataSource;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.AllDataReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReaderByTimestamp;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReader;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;

public class SeriesReaderFactory {

  private SeriesReaderFactory() {
  }

  public static SeriesReaderFactory getInstance() {
    return SeriesReaderFactoryHelper.INSTANCE;
  }

  /**
   * This method is used to create unseq file reader for IoTDB request, such as query, aggregation
   * and groupby request. Note that, job id equals -1 meant that this method is used for IoTDB merge
   * process, it's no need to maintain the opened file stream.
   */
  public PriorityMergeReader createUnSeqMergeReader(
      SeriesDataSource overflowSeriesDataSource, QueryContext context, Filter filter)
      throws IOException {

    PriorityMergeReader unSeqMergeReader = new PriorityMergeReader();

    int priorityValue = 1;

    for (TsFileResource overflowFile : overflowSeriesDataSource.getSealedFiles()) {

      TsFileSequenceReader tsFileSequenceReader = FileReaderManager.getInstance()
          .get(overflowFile.getFilePath(), true);
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileSequenceReader);
      List<ChunkMetaData> metaDataList = readFileChunkMetadata(tsFileSequenceReader, context,
          overflowSeriesDataSource.getSeriesPath(), overflowFile.getModFile());

      for (ChunkMetaData chunkMetaData : metaDataList) {
        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReader chunkReader;
        if (filter == null) {
          chunkReader = new ChunkReaderWithoutFilter(chunk);
        } else {
          chunkReader = new ChunkReaderWithFilter(chunk, filter);
        }

        unSeqMergeReader
            .addReaderWithPriority(new EngineChunkReader(chunkReader, tsFileSequenceReader),
                priorityValue);
        priorityValue++;
      }
    }

    return unSeqMergeReader;
  }

  // TODO createUnSeqMergeReaderByTime a method with filter

  /**
   * This method is used to construct reader for merge process in IoTDB. To merge only one TsFile
   * data and one UnSeqFile data.
   */
  public IReader createSeriesReaderForMerge(SeriesDataSource seqDataSource,
      SeriesDataSource overflowDataSource,
      QueryContext context)
      throws IOException {

    // sequence reader
    SequenceDataReader seqReader = new SequenceDataReader(seqDataSource, null, context);

    // unSequence merge reader
    IPointReader unSeqReader = createUnSeqMergeReader(overflowDataSource, context, null);

    return new AllDataReader(seqReader, unSeqReader);
  }

  private List<ChunkMetaData> readFileChunkMetadata(TsFileSequenceReader tsFileSequenceReader,
      QueryContext context, Path path, ModificationFile modFile) throws IOException {
    MetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(tsFileSequenceReader);
    List<ChunkMetaData> metaDataList = metadataQuerier
        .getChunkMetaDataList(path);

    List<Modification> modifications = context.getPathModifications(modFile, path.getFullPath());
    QueryUtils.modifyChunkMetaData(metaDataList, modifications);
    return metaDataList;
  }

  /**
   * construct ByTimestampReader, include sequential data and unsequential data.
   *
   * @param paths selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  public static List<EngineReaderByTimeStamp> getByTimestampReadersOfSelectedPaths(
      List<Path> paths, QueryContext context) throws IOException, StorageGroupManagerException {

    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSource queryDataSource = QueryResourceManager.getInstance().getQueryDataSource(path,
          context);

      PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

      // reader for sequence data
      SequenceDataReaderByTimestamp tsFilesReader = new SequenceDataReaderByTimestamp(
          queryDataSource.getSeqDataSource(), context);
      mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

      // reader for unSequence data
      PriorityMergeReaderByTimestamp unSeqMergeReader = SeriesReaderFactory.getInstance()
          .createUnSeqMergeReaderByTimestamp(queryDataSource.getOverflowSeriesDataSource(), context);
      mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  /**
   * This method is used to create unsequence insert reader by timestamp for IoTDB request, such as
   * query, aggregation and groupby request.
   */
  private PriorityMergeReaderByTimestamp createUnSeqMergeReaderByTimestamp(
      SeriesDataSource overflowSeriesDataSource, QueryContext context)
      throws IOException {

    PriorityMergeReaderByTimestamp unSeqMergeReader = new PriorityMergeReaderByTimestamp();

    int priorityValue = 1;

    // sealed files
    for (TsFileResource tsFileResource : overflowSeriesDataSource
        .getSealedFiles()) {

      TsFileSequenceReader fileSequenceReader = FileReaderManager.getInstance()
          .get(tsFileResource.getFilePath(), true);

      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(fileSequenceReader);
      List<ChunkMetaData> metaDataList = readFileChunkMetadata(fileSequenceReader, context,
          overflowSeriesDataSource.getSeriesPath(), tsFileResource.getModFile());

      for (ChunkMetaData chunkMetaData : metaDataList) {

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);

        unSeqMergeReader
            .addReaderWithPriority(new EngineChunkReaderByTimestamp(chunkReader), priorityValue);
        priorityValue++;
      }
    }

    // unsealed file
    if (overflowSeriesDataSource.hasUnsealedFile()) {
      UnsealedTsFile unsealedTsFile = overflowSeriesDataSource.getUnsealedFile();
      TsFileSequenceReader fileSequenceReader = FileReaderManager.getInstance()
          .get(unsealedTsFile.getFilePath(), false);
      List<ChunkMetaData> metaDataList = unsealedTsFile.getChunkMetaDataList();
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(fileSequenceReader);

      for (ChunkMetaData chunkMetaData : metaDataList) {

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);

        unSeqMergeReader
            .addReaderWithPriority(new EngineChunkReaderByTimestamp(chunkReader), priorityValue);
        priorityValue++;
      }
    }

    // MemTable
    if (overflowSeriesDataSource.hasRawSeriesChunk()) {
      unSeqMergeReader.addReaderWithPriority(
          new MemChunkReaderByTimestamp(overflowSeriesDataSource.getReadableChunk()),
          priorityValue);
    }

    // TODO add external sort when needed
    return unSeqMergeReader;
  }

  private static class SeriesReaderFactoryHelper {

    private static final SeriesReaderFactory INSTANCE = new SeriesReaderFactory();

    private SeriesReaderFactoryHelper() {
    }
  }
}