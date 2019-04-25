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

package org.apache.iotdb.db.engine.memcontrol;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemReportThread extends Thread {

  private static Logger logger = LoggerFactory.getLogger(MemReportThread.class);
  private static RecordMemController memController = RecordMemController.getInstance();

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(5 * 60 * 1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      report();
    }
  }

  public static void report() {
    Map<Object, AtomicLong> memMap = memController.getMemMap();
    StringBuilder report = new StringBuilder();
    boolean constructed = false;
    while (!constructed) {
      report = new StringBuilder();
      constructed = true;
      try {
        StringBuilder finalReport = report;
        memMap.forEach((key, value) -> finalReport.append(String.format("%s used %s%n", key.toString(),
            MemUtils.bytesCntToStr(value.get()))));
      } catch (ConcurrentModificationException e) {
        constructed = false;
      }
    }
    logger.info("Memory usages:\n{}", report);
  }
}
