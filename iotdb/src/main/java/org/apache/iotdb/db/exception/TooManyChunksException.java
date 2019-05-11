package org.apache.iotdb.db.exception;

import java.io.File;
import java.util.Arrays;

/**
 * once a TooManyChunksException is thrown, either enlarge the memory resource for queries, or begin
 * to merge the files in this exception.
 */
public class TooManyChunksException extends Exception {

  File[] files;

  public TooManyChunksException(File[] files, String device, String measurment,
      long numberOfWays) {
    super(String.format(
        "Files (%s) has too many Chunks for %s' %s, while query is only allocated %d Chunk spaces. Pls merge the OF file frist before you use it",
        Arrays.toString(files), device, measurment, numberOfWays));
    this.files = files;
  }

  public File[] getFiles() {
    return files;
  }

}
