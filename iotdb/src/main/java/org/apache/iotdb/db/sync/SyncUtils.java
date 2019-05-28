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

package org.apache.iotdb.db.sync;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.sgmanager.TsFileResource;
import org.apache.iotdb.db.sync.conf.Constans;

public class SyncUtils {

  private SyncUtils() {
    throw new UnsupportedOperationException("Initialize a util class");
  }

  /**
   * create a file link for a 'tsfile' which overlaps a sync source file represented by 'uuid'.
   * @param uuid uuid of a sync source file
   * @param tsFile a tsfile that overlaps the source file
   * @return the file link
   * @throws IOException
   */
  public static File linkFile(String uuid, TsFileResource tsFile) throws IOException {
    String relativeFilePath =
        Constans.SYNC_SERVER + File.separatorChar + uuid + File.separatorChar
            + Constans.BACK_UP_DIRECTORY_NAME
            + File.separatorChar + tsFile.getRelativePath();
    File newFile = new File(
        Directories.getInstance().getTsFileFolder(tsFile.getBaseDirIndex()),
        relativeFilePath);
    if (!newFile.getParentFile().exists()) {
      newFile.getParentFile().mkdirs();
    }
    Path link = FileSystems.getDefault().getPath(newFile.getPath());
    Path target = FileSystems.getDefault()
        .getPath(tsFile.getFile().getAbsolutePath());
    Files.createLink(link, target);

    return newFile;
  }

}
