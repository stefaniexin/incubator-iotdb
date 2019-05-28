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
package org.apache.iotdb.db.writelog.replay;
import org.apache.iotdb.db.engine.DatabaseEngineFactory;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageGroupManagerException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

public class ConcreteLogReplayer implements LogReplayer {

  /**
   * replay operation log (PhysicalPlan).
   *
   * @param plan PhysicalPlan
   * @throws ProcessorException ProcessorException
   */
  @Override
  public void replay(PhysicalPlan plan, boolean isOverflow) throws ProcessorException {
    try {
      if (plan instanceof InsertPlan) {
        InsertPlan insertPlan = (InsertPlan) plan;
        multiInsert(insertPlan);
      } else if (plan instanceof UpdatePlan) {
        UpdatePlan updatePlan = (UpdatePlan) plan;
        update(updatePlan);
      } else if (plan instanceof DeletePlan) {
        DeletePlan deletePlan = (DeletePlan) plan;
        delete(deletePlan, isOverflow);
      }
    } catch (Exception e) {
      throw new ProcessorException(
          String.format("Cannot replay log %s, because %s", plan.toString(), e.getMessage()));
    }
  }

  private void multiInsert(InsertPlan insertPlan)
      throws StorageGroupManagerException {
    DatabaseEngineFactory.getCurrent().insert(insertPlan, true);
  }

  private void update(UpdatePlan updatePlan) throws StorageGroupManagerException, PathErrorException {
    TSDataType dataType = MManager.getInstance().getSeriesType(updatePlan.getPath().getFullPath());
    for (Pair<Long, Long> timePair : updatePlan.getIntervals()) {
      DatabaseEngineFactory.getCurrent().update(updatePlan.getPath().getDevice(),
          updatePlan.getPath().getMeasurement(), timePair.left, timePair.right, dataType,
          updatePlan.getValue());
    }
  }

  private void delete(DeletePlan deletePlan, boolean isOverflow) throws StorageGroupManagerException {
    for (Path path : deletePlan.getPaths()) {
      if (isOverflow) {
        DatabaseEngineFactory.getCurrent().deleteInOverflow(path.getDevice(), path.getMeasurement(),
            deletePlan.getDeleteTime());
      } else {
        DatabaseEngineFactory.getCurrent().deleteInSeqFile(path.getDevice(), path.getMeasurement(),
            deletePlan.getDeleteTime());
      }
    }
  }
}
