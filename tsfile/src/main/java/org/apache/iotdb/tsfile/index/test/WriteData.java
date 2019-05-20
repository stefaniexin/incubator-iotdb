package org.apache.iotdb.tsfile.index.test;

import java.io.File;
import java.util.Random;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class WriteData {

  public static void main(String[] args) {

    try {

      Random random = new Random();

      String path = "test.tsfile";
      File f = new File(path);
      if (f.exists()) {
        f.delete();
      }
      TsFileWriter tsFileWriter = new TsFileWriter(f);

      // add measurements into file schema
      for (int i = 1; i <= 50; i++) {
        tsFileWriter
            .addMeasurement(new MeasurementSchema("sensor_" + i, TSDataType.FLOAT, TSEncoding.RLE));
      }
      for (int i = 51; i <= 100; i++) {
        tsFileWriter
            .addMeasurement(new MeasurementSchema("sensor_" + i, TSDataType.INT32, TSEncoding.TS_2DIFF));
      }

      for (long time = 1; time <= 10000; time++) {

        for (int deviceId = 1; deviceId <= 5; deviceId++) {

          TSRecord tsRecord = new TSRecord(time, "device_" + deviceId);

          for (int i = 1; i <= 50; i++) {
            DataPoint dPoint1 = new FloatDataPoint("sensor_" + i, 1.0f * random.nextInt(100));
            tsRecord.addTuple(dPoint1);
          }

          for (int i = 51; i <= 100; i++) {
            DataPoint dPoint2 = new IntDataPoint("sensor_" + i, random.nextInt(100));
            tsRecord.addTuple(dPoint2);
          }

          tsFileWriter.write(tsRecord);
        }

      }

      // close TsFile
      tsFileWriter.close();
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
