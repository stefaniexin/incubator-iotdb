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

package org.apache.iotdb.tsfile.expr;


import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashMapInstrumentor {

  private static final Logger LOGGER = LoggerFactory.getLogger(HashMapInstrumentor.class);

  private static final Map<Class, AtomicLong> newCounter = new ConcurrentHashMap<>();
  private static Thread reportTimer;

  public static void incCount(Class cls) {
    synchronized (newCounter) {
      AtomicLong count = newCounter.computeIfAbsent(cls, k -> new AtomicLong(0));
      count.addAndGet(1);
    }
  }

  public static void report() {
    synchronized (newCounter) {
      StringBuilder stringBuilder = new StringBuilder("\n");
      long tot = 0;
      for (Entry<Class, AtomicLong> entry : newCounter.entrySet()) {
        long count = entry.getValue().get();
        if (count < 100) {
          continue;
        }
        stringBuilder.append(entry.getKey().getSimpleName()).append(":").append(count).append("\n");
        tot += count;
      }
      stringBuilder.append("total:").append(tot);
      LOGGER.info("HashMap usage:\n{}", stringBuilder);
    }
  }

  public static void start() {
    reportTimer = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(5*60*1000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        report();
      }
    });
    reportTimer.start();
  }

  public static void stop() {
    if (reportTimer != null) {
      reportTimer.interrupt();
    }
  }
}
