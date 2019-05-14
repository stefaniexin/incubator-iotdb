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
