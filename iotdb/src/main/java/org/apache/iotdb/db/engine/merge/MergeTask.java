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

package org.apache.iotdb.db.engine.merge;

import static java.time.ZonedDateTime.ofInstant;

import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.sgmanager.StorageGroupProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeTask implements Callable<Exception> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MergeTask.class);

  private StorageGroupProcessor processor;

  public MergeTask(StorageGroupProcessor processor) {
    this.processor = processor;
  }

  @Override
  public Exception call() {
    try {
      ZoneId zoneId = IoTDBDescriptor.getInstance().getConfig().getZoneID();
      long mergeStartTime = System.currentTimeMillis();
      processor.merge();
      long mergeEndTime = System.currentTimeMillis();
      long intervalTime = mergeEndTime - mergeStartTime;
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(
            "The filenode processor {} merge start time is {}, "
                + "merge end time is {}, merge consumes {}ms.",
            processor.getProcessorName(), ofInstant(Instant.ofEpochMilli(mergeStartTime),
                zoneId), ofInstant(Instant.ofEpochMilli(mergeEndTime),
                zoneId), intervalTime);
      }

    } catch (Exception e) {
      return e;
    }
    return null;
  }
}
