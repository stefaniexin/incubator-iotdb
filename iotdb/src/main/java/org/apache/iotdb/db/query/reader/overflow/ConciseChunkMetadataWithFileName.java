package org.apache.iotdb.db.query.reader.overflow;

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

public class ConciseChunkMetadataWithFileName extends ConciseChunkMetadata {

  File file;

  public ConciseChunkMetadataWithFileName(long startTime, long fileOffset, File file) {
    super(startTime, fileOffset);
    this.file = file;
  }

  public ConciseChunkMetadataWithFileName(ChunkMetaData metaData, File file) {
    super(metaData);
    this.file = file;
  }

  public File getFile() {
    return file;
  }
}
