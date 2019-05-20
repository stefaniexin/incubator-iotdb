package org.apache.iotdb.tsfile.index.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

public class ReadData {

  private static final String FILE_PATH = "/Users/beyyes/Desktop/99.tsfile";

  public static void main(String[] args) throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    TsFileMetaData metaData = reader.readFileMetadata();
    List<Pair<Long, Long>> offsetList = new ArrayList<>();
    long startOffset = reader.position();
    byte marker;
    while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
          ChunkHeader header = reader.readChunkHeader();

          for (int j = 0; j < header.getNumOfPages(); j++) {
            PageHeader pageHeader = reader.readPageHeader(header.getDataType());
            System.out.println(String.format("page compressed size: %s, page point number %s",
                pageHeader.getCompressedSize(), pageHeader.getNumOfValues()));
            reader.readPage(pageHeader, header.getCompressionType());
          }

          break;
        case MetaMarker.CHUNK_GROUP_FOOTER:
          reader.readChunkGroupFooter();
          long endOffset = reader.position();
          offsetList.add(new Pair<>(startOffset, endOffset));
          startOffset = endOffset;
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    int offsetListIndex = 0;
    List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata.getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        Pair<Long, Long> pair = offsetList.get(offsetListIndex++);
        System.out.println(String.format("%s %s", chunkGroupMetaData.getStartOffsetOfChunkGroup(), pair.left));
        System.out.println(String.format("%s %s", chunkGroupMetaData.getEndOffsetOfChunkGroup(), pair.right));
      }
    }
    reader.close();
  }

}
