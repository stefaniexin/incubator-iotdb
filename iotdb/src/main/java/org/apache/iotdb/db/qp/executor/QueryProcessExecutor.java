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
package org.apache.iotdb.db.qp.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

public abstract class QueryProcessExecutor {

  protected ThreadLocal<Integer> fetchSize = new ThreadLocal<>();
  protected EngineQueryRouter queryRouter = new EngineQueryRouter();

  public QueryProcessExecutor() {
  }

  /**
   * process query plan of qp layer, construct queryDataSet.
   * @param queryPlan QueryPlan
   * @return QueryDataSet
   */
  public QueryDataSet processQuery(QueryPlan queryPlan, QueryContext context)
      throws IOException, StorageGroupManagerException, PathErrorException,
      QueryFilterOptimizationException, ProcessorException {

    QueryExpression queryExpression = QueryExpression.create().setSelectSeries(queryPlan.getPaths())
        .setExpression(queryPlan.getExpression());
    if (queryPlan instanceof GroupByPlan) {
      GroupByPlan groupByPlan = (GroupByPlan) queryPlan;
      return groupBy(groupByPlan.getPaths(), groupByPlan.getAggregations(),
          groupByPlan.getExpression(), groupByPlan.getUnit(), groupByPlan.getOrigin(),
          groupByPlan.getIntervals(), context);
    }

    if (queryPlan instanceof AggregationPlan) {
      return aggregate(queryPlan.getPaths(), queryPlan.getAggregations(),
          ((AggregationPlan) queryPlan).getExpression(), context);
    }

    if (queryPlan instanceof FillQueryPlan) {
      FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
      return fill(queryPlan.getPaths(), fillQueryPlan.getQueryTime(),
          fillQueryPlan.getFillType(), context);
    }
    return queryRouter.query(queryExpression, context);
  }

  public abstract TSDataType getSeriesType(Path fullPath) throws PathErrorException;

  public abstract boolean judgePathExists(Path fullPath);

  public boolean processNonQuery(PhysicalPlan plan) throws ProcessorException {
    throw new UnsupportedOperationException();
  }

  public int getFetchSize() {
    if (fetchSize.get() == null) {
      return 100;
    }
    return fetchSize.get();
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize.set(fetchSize);
  }

  public abstract QueryDataSet aggregate(List<Path> paths, List<String> aggres,
      IExpression expression, QueryContext context) throws ProcessorException, IOException,
      PathErrorException, StorageGroupManagerException, QueryFilterOptimizationException;

  public abstract QueryDataSet groupBy(List<Path> paths, List<String> aggres,
      IExpression expression, long unit, long origin, List<Pair<Long, Long>> intervals,
      QueryContext context) throws ProcessorException, IOException, PathErrorException,
      StorageGroupManagerException, QueryFilterOptimizationException;

  public abstract QueryDataSet fill(List<Path> fillPaths, long queryTime, Map<TSDataType,
      IFill> fillTypes, QueryContext context)
      throws ProcessorException, IOException, PathErrorException, StorageGroupManagerException;

  /**
   * executeWithGlobalTimeFilter update command and return whether the operator is successful.
   *
   * @param path : update series seriesPath
   * @param startTime start time in update command
   * @param endTime end time in update command
   * @param value - in type of string
   * @return - whether the operator is successful.
   */
  public abstract boolean update(Path path, long startTime, long endTime, String value)
      throws ProcessorException;

  /**
   * executeWithGlobalTimeFilter delete command and return whether the operator is successful.
   *
   * @param paths : delete series paths
   * @param deleteTime end time in delete command
   * @return - whether the operator is successful.
   */
  public boolean delete(List<Path> paths, long deleteTime) throws ProcessorException {
    try {
      boolean result = true;
      MManager mManager = MManager.getInstance();
      Set<String> pathSet = new HashSet<>();
      for (Path p : paths) {
        pathSet.addAll(mManager.getPaths(p.getFullPath()));
      }
      if (pathSet.isEmpty()) {
        throw new ProcessorException("TimeSeries does not exist and cannot be delete data");
      }
      for (String onePath : pathSet) {
        if (!mManager.pathExist(onePath)) {
          throw new ProcessorException(
              String.format("TimeSeries %s does not exist and cannot be delete its data", onePath));
        }
      }
      List<String> fullPath = new ArrayList<>();
      fullPath.addAll(pathSet);
      for (String path : fullPath) {
        result &= delete(new Path(path), deleteTime);
      }
      return result;
    } catch (PathErrorException e) {
      throw new ProcessorException(e.getMessage());
    }
  }

  /**
   * executeWithGlobalTimeFilter delete command and return whether the operator is successful.
   *
   * @param path : delete series seriesPath
   * @param deleteTime end time in delete command
   * @return - whether the operator is successful.
   */
  protected abstract boolean delete(Path path, long deleteTime) throws ProcessorException;

  /**
   * execute insert command and return whether the operator is successful.
   *
   * @param plan the InsertPlan
   */
  public abstract void multiInsert(InsertPlan plan) throws ProcessorException;

  public abstract List<String> getAllPaths(String originPath) throws PathErrorException;

}
