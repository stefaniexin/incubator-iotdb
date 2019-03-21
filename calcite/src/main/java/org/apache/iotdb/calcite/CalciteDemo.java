package org.apache.iotdb.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CalciteDemo {

  /**
   * Very simple example how to read a tsfile over calcite jdbc.
   */
  public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
    // file path
    String path = "calcite/src/test/resources/test.tsfile";

    // read example : no filter
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    final TsFileMetaData tsFileMetaData = reader.readFileMetadata();
    for (String device : tsFileMetaData.getDeviceMap().keySet()) {
      System.out.println("Available devices: " + device);
    }

    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    Schema schema = new AbstractSchema() {
      @Override protected Map<String, Table> getTableMap() {
        return tsFileMetaData.getDeviceMap().keySet().stream()
            .collect(Collectors.toMap(
                Function.identity(),
                device -> new DeviceTable(reader, device, tsFileMetaData.getMeasurementSchema())
            ));
      }
    };
    rootSchema.add("devices", schema);
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from devices.device_1 limit 10");
    while (resultSet.next()) {
      System.out.println(resultSet.getLong("timestamp") + "," + resultSet.getDouble(1) + "," + resultSet.getDouble("sensor_20"));
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  /**
   * Table that represents one Device.
   * The Columns are
   * - the timestamp (as double)
   * - one channel per sensor in the file
   */
  public static class DeviceTable extends AbstractTable implements ScannableTable {

    private final TsFileSequenceReader reader;
    private final String deviceName;
    private final Map<String, MeasurementSchema> measurementSchema;
    private final List<String> channels;

    public DeviceTable(TsFileSequenceReader reader, String deviceName, Map<String, MeasurementSchema> measurementSchema) {
      this.reader = reader;
      this.deviceName = deviceName;
      this.measurementSchema = measurementSchema;
      // Make a "ordered list of the channels"
      channels = measurementSchema.keySet().stream().sorted().collect(Collectors.toList());
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      builder.add("timestamp", SqlTypeName.BIGINT);
      for (String channel : channels) {
        builder.add(channel, toCalciteType(measurementSchema.get(channel).getType()));
      }

      return builder.build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(createEnumerator());
    }

    private QueryExpressionEnumerable createEnumerator() {
      ReadOnlyTsFile readTsFile = null;
      try {
        readTsFile = new ReadOnlyTsFile(reader);
        final List<Path> paths = channels.stream()
            .map(channel -> new Path(deviceName, channel))
            .collect(Collectors.toList());
        QueryExpression queryExpression = QueryExpression.create(paths, null);
        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        return new QueryExpressionEnumerable(readTsFile, paths, queryDataSet);
      } catch (IOException e) {
        throw new RuntimeException("Error on reading file", e);
      }
    }

    static SqlTypeName toCalciteType(TSDataType tsDataType) {
      switch (tsDataType) {
        case BOOLEAN:
          return SqlTypeName.BOOLEAN;
        case FLOAT:
          return SqlTypeName.FLOAT;
        case INT32:
          return SqlTypeName.INTEGER;
        case INT64:
          return SqlTypeName.BIGINT;
        case DOUBLE:
          return SqlTypeName.DOUBLE;
        case TEXT:
          return SqlTypeName.VARCHAR;
        default:
          throw new NotImplementedException("TSDataType " + tsDataType + " is currently not implemented!");
      }
    }
  }

  /**
   * Helper class that creates an enumerable from a {@link QueryExpression}.
   */
  static class QueryExpressionEnumerable extends AbstractEnumerable {

    private final ReadOnlyTsFile readTsFile;
    private final List<Path> paths;
    private final QueryDataSet queryDataSet;

    public QueryExpressionEnumerable(ReadOnlyTsFile readTsFile, List<Path> paths, QueryDataSet queryDataSet) {
      this.readTsFile = readTsFile;
      this.paths = paths;
      this.queryDataSet = queryDataSet;
    }

    @Override public Enumerator<Object[]> enumerator() {
      return new Enumerator<Object[]>() {

        private RowRecord current = null;

        @Override public Object[] current() {
          final Object[] result = new Object[paths.size() + 1];
          result[0] = current.getTimestamp();
          for (int i = 0; i < paths.size(); i++) {
            result[i + 1] = getFieldAsObject(i);
          }
          return result;
        }

        private Object getFieldAsObject(int i) {
          final Field field = current.getFields().get(i);
          if (field.isNull()) {
            return null;
          }
          switch (field.getDataType()) {
            case BOOLEAN:
              return field.getBoolV();
            case DOUBLE:
              return field.getDoubleV();
            case INT64:
              return field.getLongV();
            case INT32:
              return field.getIntV();
            case FLOAT:
              return field.getFloatV();
            case TEXT:
              return field.getStringValue();
            default:
              throw new NotImplementedException("No mapping defined for DataType " + field.getDataType());
          }
        }

        @Override public boolean moveNext() {
          try {
            final boolean hasNext = queryDataSet.hasNext();
            if (hasNext) {
              current = queryDataSet.next();
            }
            return hasNext;
          } catch (IOException e) {
            throw new IllegalStateException("This should not happen");
          }
        }

        @Override public void reset() {
          // noop
        }

        @Override public void close() {
          // close file
          try {
            readTsFile.close();
          } catch (IOException e) {
            // noop
          }
        }
      };
    }
  }

}
