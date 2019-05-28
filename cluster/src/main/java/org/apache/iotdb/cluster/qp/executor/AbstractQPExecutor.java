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
package org.apache.iotdb.cluster.qp.executor;

import com.alipay.sofa.jraft.entity.PeerId;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.exception.ConsistencyLevelException;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.task.QPTask;
import org.apache.iotdb.cluster.qp.task.QPTask.TaskState;
import org.apache.iotdb.cluster.qp.task.SingleQPTask;
import org.apache.iotdb.cluster.rpc.raft.impl.RaftNodeAsClientManager;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.cluster.utils.hash.Router;
import org.apache.iotdb.db.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQPExecutor.class);

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();

  protected Router router = Router.getInstance();

  protected MManager mManager = MManager.getInstance();

  protected final Server server = Server.getInstance();

  /**
   * The task in progress.
   */
  protected ThreadLocal<QPTask> currentTask = new ThreadLocal<>();

  /**
   * Count limit to redo a single task
   */
  private static final int TASK_MAX_RETRY = CLUSTER_CONFIG.getQpTaskRedoCount();

  /**
   * ReadMetadataConsistencyLevel: 1 Strong consistency, 2 Weak consistency
   */
  private ThreadLocal<Integer> readMetadataConsistencyLevel = new ThreadLocal<>();

  /**
   * ReadDataConsistencyLevel: 1 Strong consistency, 2 Weak consistency
   */
  private ThreadLocal<Integer> readDataConsistencyLevel = new ThreadLocal<>();

  public AbstractQPExecutor() {
  }

  /**
   * Check init of consistency level(<code>ThreadLocal</code>)
   */
  private void checkInitConsistencyLevel() {
    if (readMetadataConsistencyLevel.get() == null) {
      readMetadataConsistencyLevel.set(CLUSTER_CONFIG.getReadMetadataConsistencyLevel());
    }
    if (readDataConsistencyLevel.get() == null) {
      readDataConsistencyLevel.set(CLUSTER_CONFIG.getReadDataConsistencyLevel());
    }
  }

  /**
   * Async handle QPTask by QPTask and leader id
   *
   * @param task request QPTask
   * @param taskRetryNum Number of QPTask retries due to timeout and redirected.
   * @return basic response
   */
  private BasicResponse syncHandleSingleTaskGetRes(SingleQPTask task, int taskRetryNum, String taskInfo, String groupId, Set<PeerId> downNodeSet)
      throws InterruptedException, RaftConnectionException {
    PeerId firstNode = task.getTargetNode();
    RaftUtils.updatePeerIDOrder(firstNode, groupId);
    BasicResponse response = null;
    try {
      asyncSendSingleTask(task, taskRetryNum);
      response = syncGetSingleTaskRes(task, taskRetryNum, taskInfo, groupId, downNodeSet);
    } catch (RaftConnectionException ex) {
      boolean success = false;
      downNodeSet.add(firstNode);
      while (!success) {
        PeerId nextNode = null;
        try {
          nextNode = RaftUtils.getPeerIDInOrder(groupId);
          if (firstNode.equals(nextNode)) {
            break;
          }
          LOGGER.debug(
              "Previous task fail, then send {} task for group {} to node {}.", taskInfo, groupId,
              nextNode);
          task.resetTask();
          task.setTargetNode(nextNode);
          task.setTaskState(TaskState.INITIAL);
          asyncSendSingleTask(task, taskRetryNum);
          response = syncGetSingleTaskRes(task, taskRetryNum, taskInfo, groupId, downNodeSet);
          LOGGER.debug("{} task for group {} to node {} succeed.", taskInfo, groupId, nextNode);
          success = true;
        } catch (RaftConnectionException e1) {
          LOGGER.debug("{} task for group {} to node {} fail.", taskInfo, groupId, nextNode);
          downNodeSet.add(nextNode);
        }
      }
      LOGGER.debug("The final result for {} task is {}", taskInfo, success);
      if (!success) {
        throw ex;
      }
    }
    return response;
  }

  protected BasicResponse syncHandleSingleTaskGetRes(SingleQPTask task, int taskRetryNum, String taskInfo, String groupId)
      throws RaftConnectionException, InterruptedException {
    return syncHandleSingleTaskGetRes(task, taskRetryNum, taskInfo, groupId, new HashSet<>());
  }

  /**
   * Asynchronous send rpc task via client
   *  @param task rpc task
   * @param taskRetryNum Retry time of the task
   */
  protected void asyncSendSingleTask(SingleQPTask task, int taskRetryNum)
      throws RaftConnectionException {
    if (taskRetryNum >= TASK_MAX_RETRY) {
      throw new RaftConnectionException(String.format("QPTask retries reach the upper bound %s",
          TASK_MAX_RETRY));
    }
    RaftNodeAsClientManager.getInstance().produceQPTask(task);
  }

  /**
   * Synchronous get task response. If it's redirected or status is exception, the task needs to be
   * resent. Note: If status is Exception, it marks that an exception occurred during the task is
   * being sent instead of executed.
   * @param task rpc task
   * @param taskRetryNum Retry time of the task
   */
  private BasicResponse syncGetSingleTaskRes(SingleQPTask task, int taskRetryNum, String taskInfo, String groupId, Set<PeerId> downNodeSet)
      throws InterruptedException, RaftConnectionException {
    task.await();
    PeerId leader;
    if (task.getTaskState() != TaskState.FINISH) {
      if (task.getTaskState() == TaskState.RAFT_CONNECTION_EXCEPTION) {
        throw new RaftConnectionException(
            String.format("Can not connect to remote node : %s", task.getTargetNode()));
      } else if (task.getTaskState() == TaskState.REDIRECT) {
        // redirect to the right leader
        leader = PeerId.parsePeer(task.getResponse().getLeaderStr());

        if (downNodeSet.contains(leader)) {
          LOGGER.debug("Redirect leader {} is down, group {} might be down.", leader, groupId);
          throw new RaftConnectionException(
              String.format("Can not connect to leader of remote node : %s", task.getTargetNode()));
        } else {
          LOGGER
              .debug("Redirect leader: {}, group id = {}", leader, task.getRequest().getGroupID());
          RaftUtils.updateRaftGroupLeader(task.getRequest().getGroupID(), leader);
        }
      } else {
        RaftUtils.removeCachedRaftGroupLeader(groupId);
        LOGGER.debug("Remove cached raft group leader of {}", groupId);
        leader = RaftUtils.getLocalLeaderPeerID(groupId);
      }
      task.setTargetNode(leader);
      task.resetTask();
      return syncHandleSingleTaskGetRes(task, taskRetryNum + 1, taskInfo, groupId, downNodeSet);
    }
    return task.getResponse();
  }

  public void shutdown() {
    if (currentTask.get() != null) {
      currentTask.get().shutdown();
    }
  }

  public void setReadMetadataConsistencyLevel(int level) throws ConsistencyLevelException {
    if (level <= ClusterConstant.MAX_CONSISTENCY_LEVEL) {
      readMetadataConsistencyLevel.set(level);
    } else {
      throw new ConsistencyLevelException(String.format("Consistency level %d not support", level));
    }
  }

  public void setReadDataConsistencyLevel(int level) throws ConsistencyLevelException {
    if (level <= ClusterConstant.MAX_CONSISTENCY_LEVEL) {
      readDataConsistencyLevel.set(level);
    } else {
      throw new ConsistencyLevelException(String.format("Consistency level %d not support", level));
    }
  }

  public int getReadMetadataConsistencyLevel() {
    checkInitConsistencyLevel();
    return readMetadataConsistencyLevel.get();
  }

  public int getReadDataConsistencyLevel() {
    checkInitConsistencyLevel();
    return readDataConsistencyLevel.get();
  }

  /**
   * Async handle task by SingleQPTask and leader id.
   *
   * @param task request SingleQPTask
   * @return request result
   */
  public boolean syncHandleSingleTask(SingleQPTask task, String taskInfo, String groupId)
      throws RaftConnectionException, InterruptedException {
    BasicResponse response = syncHandleSingleTaskGetRes(task, 0, taskInfo, groupId);
    return response != null && response.isSuccess();
  }
}
