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
    AtomicLong count = newCounter.computeIfAbsent(cls, k -> new AtomicLong(0));
    count.addAndGet(1);
  }

  public static void report() {
    StringBuilder stringBuilder = new StringBuilder("\n");
    long tot = 0;
    for (Entry<Class, AtomicLong> entry : newCounter.entrySet()) {
      long count = entry.getValue().get();
      stringBuilder.append(entry.getKey().getSimpleName()).append(":").append(count).append("\n");
      tot += count;
    }
    stringBuilder.append("total:").append(tot);
    LOGGER.info("HashMap usage:\n{}", stringBuilder);
  }

  public static void start() {
    reportTimer = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(60*1000);
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
