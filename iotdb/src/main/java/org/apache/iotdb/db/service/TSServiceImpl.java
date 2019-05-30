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
package org.apache.iotdb.db.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.IllegalASTFormatException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.Metadata;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSHandleIdentifier;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thrift RPC implementation at server side.
 */

public class TSServiceImpl implements TSIService.Iface, ServerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);
  protected static final String INFO_NOT_LOGIN = "{}: Not login.";
  protected static final String ERROR_NOT_LOGIN = "Not login";

  protected QueryProcessor processor;
  // Record the username for every rpc connection. Username.get() is null if
  // login is failed.
  protected ThreadLocal<String> username = new ThreadLocal<>();
  protected ThreadLocal<HashMap<String, PhysicalPlan>> queryStatus = new ThreadLocal<>();
  protected ThreadLocal<HashMap<String, QueryDataSet>> queryRet = new ThreadLocal<>();
  protected ThreadLocal<ZoneId> zoneIds = new ThreadLocal<>();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected ThreadLocal<Map<Long, QueryContext>> contextMapLocal = new ThreadLocal<>();

  public TSServiceImpl() throws IOException {
    processor = new QueryProcessor(new OverflowQPExecutor());
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    LOGGER.info("{}: receive open session request from username {}", IoTDBConstant.GLOBAL_DB_NAME,
        req.getUsername());

    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    try {
      status = authorizer.login(req.getUsername(), req.getPassword());
    } catch (AuthException e) {
      LOGGER.error("meet error while logging in.", e);
      status = false;
    }
    TS_Status tsStatus;
    if (status) {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      tsStatus.setErrorMessage("login successfully.");
      username.set(req.getUsername());
      zoneIds.set(config.getZoneID());
      initForOneSession();
    } else {
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage("login failed. Username or password is wrong.");
    }
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
        TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1);
    resp.setSessionHandle(
        new TS_SessionHandle(new TSHandleIdentifier(ByteBuffer.wrap(req.getUsername().getBytes()),
            ByteBuffer.wrap(req.getPassword().getBytes()))));
    LOGGER.info("{}: Login status: {}. User : {}", IoTDBConstant.GLOBAL_DB_NAME,
        tsStatus.getErrorMessage(),
        req.getUsername());

    return resp;
  }

  private void initForOneSession() {
    queryStatus.set(new HashMap<>());
    queryRet.set(new HashMap<>());
  }

  @Override
  public TSCloseSessionResp closeSession(TSCloseSessionReq req) throws TException {
    LOGGER.info("{}: receive close session", IoTDBConstant.GLOBAL_DB_NAME);
    TS_Status tsStatus;
    if (username.get() == null) {
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage("Has not logged in");
      if (zoneIds.get() != null) {
        zoneIds.remove();
      }
    } else {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      username.remove();
      if (zoneIds.get() != null) {
        zoneIds.remove();
      }
    }
    return new TSCloseSessionResp(tsStatus);
  }

  @Override
  public TSCancelOperationResp cancelOperation(TSCancelOperationReq req) throws TException {
    return new TSCancelOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSCloseOperationResp closeOperation(TSCloseOperationReq req) throws TException {
    LOGGER.info("{}: receive close operation", IoTDBConstant.GLOBAL_DB_NAME);
    try {

      releaseQueryResource(req);

      clearAllStatusForCurrentRequest();
    } catch (Exception e) {
      LOGGER.error("Error in closeOperation : ", e);
    }
    return new TSCloseOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
  }

  protected void releaseQueryResource(TSCloseOperationReq req) throws Exception {
    Map<Long, QueryContext> contextMap = contextMapLocal.get();
    if (contextMap == null) {
      return;
    }
    if (req == null || req.queryId == -1) {
      // end query for all the query tokens created by current thread
      for (QueryContext context : contextMap.values()) {
        QueryResourceManager.getInstance().endQueryForGivenJob(context.getJobId());
      }
      contextMapLocal.set(new HashMap<>());
    } else {
      QueryResourceManager.getInstance()
          .endQueryForGivenJob(contextMap.remove(req.queryId).getJobId());
    }
  }

  private void clearAllStatusForCurrentRequest() {
    if (this.queryRet.get() != null) {
      this.queryRet.get().clear();
    }
    if (this.queryStatus.get() != null) {
      this.queryStatus.get().clear();
    }
  }

  private TS_Status getErrorStatus(String message) {
    TS_Status status = new TS_Status(TS_StatusCode.ERROR_STATUS);
    status.setErrorMessage(message);
    return status;
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) throws TException {
    TS_Status status;
    if (!checkLogin()) {
      LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = getErrorStatus(ERROR_NOT_LOGIN);
      return new TSFetchMetadataResp(status);
    }
    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    switch (req.getType()) {
      case "SHOW_TIMESERIES":
        String path = req.getColumnPath();
        try {
          List<List<String>> showTimeseriesList = getTimeSeriesForPath(path);
          resp.setShowTimeseriesList(showTimeseriesList);
        } catch (PathErrorException | InterruptedException | ProcessorException e) {
          status = getErrorStatus(
              String.format("Failed to fetch timeseries %s's metadata because: %s",
                  req.getColumnPath(), e));
          resp.setStatus(status);
          return resp;
        } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
          LOGGER
              .error(String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()),
                  outOfMemoryError);
          status = getErrorStatus(
              String.format("Failed to fetch timeseries %s's metadata because: %s",
                  req.getColumnPath(), outOfMemoryError));
          resp.setStatus(status);
          return resp;
        }
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      case "SHOW_STORAGE_GROUP":
        try {
          Set<String> storageGroups = getAllStorageGroups();
          resp.setShowStorageGroups(storageGroups);
        } catch (PathErrorException | InterruptedException e) {
          status = getErrorStatus(
              String.format("Failed to fetch storage groups' metadata because: %s", e));
          resp.setStatus(status);
          return resp;
        } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
          LOGGER.error("Failed to fetch storage groups' metadata", outOfMemoryError);
          status = getErrorStatus(
              String.format("Failed to fetch storage groups' metadata because: %s",
                  outOfMemoryError));
          resp.setStatus(status);
          return resp;
        }
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      case "METADATA_IN_JSON":
        String metadataInJson;
        try {
          metadataInJson = getMetadataInString();
        } catch (PathErrorException | InterruptedException | ProcessorException e) {
          status = getErrorStatus(
              String.format("Failed to fetch all metadata in json because: %s", e));
          resp.setStatus(status);
          return resp;
        } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
          LOGGER.error("Failed to fetch all metadata in json", outOfMemoryError);
          status = getErrorStatus(
              String.format("Failed to fetch all metadata in json because: %s", outOfMemoryError));
          resp.setStatus(status);
          return resp;
        }
        resp.setMetadataInJson(metadataInJson);
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      case "DELTA_OBEJECT":
        Metadata metadata;
        try {
          String column = req.getColumnPath();
          metadata = getMetadata();
          Map<String, List<String>> deviceMap = metadata.getDeviceMap();
          if (deviceMap == null || !deviceMap.containsKey(column)) {
            resp.setColumnsList(new ArrayList<>());
          } else {
            resp.setColumnsList(deviceMap.get(column));
          }
        } catch (PathErrorException | InterruptedException | ProcessorException e) {
          LOGGER.error("cannot get delta object map", e);
          status = getErrorStatus(String.format("Failed to fetch delta object map because: %s", e));
          resp.setStatus(status);
          return resp;
        } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
          LOGGER.error("Failed to get delta object map", outOfMemoryError);
          status = getErrorStatus(
              String.format("Failed to get delta object map because: %s", outOfMemoryError));
          break;
        }
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      case "COLUMN":
        try {
          resp.setDataType(getSeriesType(req.getColumnPath()).toString());
        } catch (PathErrorException | InterruptedException | ProcessorException e) {
          // TODO aggregate seriesPath e.g. last(root.ln.wf01.wt01.status)
          // status = new TS_Status(TS_StatusCode.ERROR_STATUS);
          // status.setErrorMessage(String.format("Failed to fetch %s's data type because: %s",
          // req.getColumnPath(), e));
          // resp.setStatus(status);
          // return resp;
        }
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      case "ALL_COLUMNS":
        try {
          resp.setColumnsList(getPaths(req.getColumnPath()));
        } catch (PathErrorException | InterruptedException | ProcessorException e) {
          status = getErrorStatus(String
              .format("Failed to fetch %s's all columns because: %s", req.getColumnPath(), e));
          resp.setStatus(status);
          return resp;
        } catch (OutOfMemoryError outOfMemoryError) { // TODO OOME
          LOGGER.error("Failed to fetch seriesPath {}'s all columns", req.getColumnPath(),
              outOfMemoryError);
          status = getErrorStatus(String.format("Failed to fetch %s's all columns because: %s",
              req.getColumnPath(), outOfMemoryError));
          break;
        }
        status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
        break;
      default:
        status = new TS_Status(TS_StatusCode.ERROR_STATUS);
        status
            .setErrorMessage(String.format("Unsuport fetch metadata operation %s", req.getType()));
        break;
    }
    resp.setStatus(status);
    return resp;
  }

  protected Set<String> getAllStorageGroups() throws PathErrorException, InterruptedException {
    return MManager.getInstance().getAllStorageGroup();
  }

  protected List<List<String>> getTimeSeriesForPath(String path)
      throws PathErrorException, InterruptedException, ProcessorException {
    return MManager.getInstance().getShowTimeseriesPath(path);
  }

  protected String getMetadataInString()
      throws InterruptedException, PathErrorException, ProcessorException {
    return MManager.getInstance().getMetadataInString();
  }

  protected Metadata getMetadata()
      throws PathErrorException, InterruptedException, ProcessorException {
    return MManager.getInstance().getMetadata();
  }

  protected TSDataType getSeriesType(String path)
      throws PathErrorException, InterruptedException, ProcessorException {
    return MManager.getInstance().getSeriesType(path);
  }

  protected List<String> getPaths(String path)
      throws PathErrorException, InterruptedException, ProcessorException {
    return MManager.getInstance().getPaths(path);
  }

  /**
   * Judge whether the statement is ADMIN COMMAND and if true, execute it.
   *
   * @param statement command
   * @return true if the statement is ADMIN COMMAND
   * @throws IOException exception
   */
  private boolean execAdminCommand(String statement) throws IOException {
    if (!"root".equals(username.get())) {
      return false;
    }
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase();
    switch (statement) {
      case "flush":
        try {
          FileNodeManager.getInstance().closeAll();
        } catch (FileNodeManagerException e) {
          LOGGER.error("meet error while FileNodeManager closing all!", e);
          throw new IOException(e);
        }
        return true;
      case "merge":
        try {
          FileNodeManager.getInstance().mergeAll();
        } catch (FileNodeManagerException e) {
          LOGGER.error("meet error while FileNodeManager merging all!", e);
          throw new IOException(e);
        }
        return true;
      default:
        return false;
    }
  }

  @Override
  public TSExecuteBatchStatementResp executeBatchStatement(TSExecuteBatchStatementReq req)
      throws TException {
    try {
      if (!checkLogin()) {
        LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN, null);
      }
      List<String> statements = req.getStatements();
      List<Integer> result = new ArrayList<>();
      boolean isAllSuccessful = true;
      String batchErrorMessage = "";

      for (String statement : statements) {
        try {
          PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
          physicalPlan.setProposer(username.get());
          if (physicalPlan.isQuery()) {
            return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
                "statement is query :" + statement, result);
          }
          TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan);
          if (resp.getStatus().getStatusCode().equals(TS_StatusCode.SUCCESS_STATUS)) {
            result.add(Statement.SUCCESS_NO_INFO);
          } else {
            result.add(Statement.EXECUTE_FAILED);
            isAllSuccessful = false;
            batchErrorMessage = resp.getStatus().getErrorMessage();
          }
        } catch (Exception e) {
          String errMessage = String.format(
              "Fail to generate physcial plan and execute for statement "
                  + "%s beacuse %s",
              statement, e.getMessage());
          LOGGER.warn("Error occurred when executing {}", statement, e);
          result.add(Statement.EXECUTE_FAILED);
          isAllSuccessful = false;
          batchErrorMessage = errMessage;
        }
      }
      if (isAllSuccessful) {
        return getTSBathExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS,
            "Execute batch statements successfully", result);
      } else {
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, batchErrorMessage, result);
      }
    } catch (Exception e) {
      LOGGER.error("{}: error occurs when executing statements", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage(), null);
    }
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) throws TException {
    try {
      if (!checkLogin()) {
        LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
      }
      String statement = req.getStatement();

      try {
        if (execAdminCommand(statement)) {
          return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "ADMIN_COMMAND_SUCCESS");
        }
      } catch (Exception e) {
        LOGGER.error("meet error while executing admin command!", e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
      }

      try {
        if (execSetConsistencyLevel(statement)) {
          return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS,
              "Execute set consistency level successfully");
        }
      } catch (Exception e) {
        LOGGER.error("Error occurred when executing statement {}", statement, e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
      }

      PhysicalPlan physicalPlan;
      try {
        physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
        physicalPlan.setProposer(username.get());
      } catch (IllegalASTFormatException e) {
        LOGGER.debug("meet error while parsing SQL to physical plan: ", e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "Statement format is not right:" + e.getMessage());
      } catch (NullPointerException e) {
        LOGGER.error("meet error while parsing SQL to physical plan: ", e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Statement is not allowed");
      }
      if (physicalPlan.isQuery()) {
        return executeQueryStatement(req);
      } else {
        return executeUpdateStatement(physicalPlan);
      }
    } catch (Exception e) {
      LOGGER.info("meet error while executing statement: {}", req.getStatement(), e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  /**
   * Set consistency level
   */
  public boolean execSetConsistencyLevel(String statement) throws Exception {
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase().trim();
    if (Pattern.matches(IoTDBConstant.SET_READ_CONSISTENCY_LEVEL_PATTERN, statement)) {
      throw new Exception(
          "IoTDB Stand-alone version does not support setting read-write consistency level");
    } else {
      return false;
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) throws TException {

    try {
      if (!checkLogin()) {
        LOGGER.info("{}: Not login.", IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login");
      }

      String statement = req.getStatement();
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      plan.setProposer(username.get());

      List<Path> paths;
      paths = plan.getPaths();

      // check seriesPath exists
      if (paths.isEmpty()) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Timeseries does not exist.");
      }

      // check file level set

      try {
        checkFileLevelSet(paths);
      } catch (PathErrorException e) {
        LOGGER.error("meet error while checking file level.", e);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
      }

      // check permissions
      if (!checkAuthorization(paths, plan)) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "No permissions for this query.");
      }

      TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
      List<String> columns = new ArrayList<>();
      // Restore column header of aggregate to func(column_name), only
      // support single aggregate function for now
      if (plan instanceof QueryPlan) {
        switch (plan.getOperatorType()) {
          case QUERY:
          case FILL:
            for (Path p : paths) {
              columns.add(p.getFullPath());
            }
            break;
          case AGGREGATION:
          case GROUPBY:
            List<String> aggregations = plan.getAggregations();
            if (aggregations.size() != paths.size()) {
              for (int i = 1; i < paths.size(); i++) {
                aggregations.add(aggregations.get(0));
              }
            }
            for (int i = 0; i < paths.size(); i++) {
              columns.add(aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")");
            }
            break;
          default:
            throw new TException("unsupported query type: " + plan.getOperatorType());
        }
      } else {
        Operator.OperatorType type = plan.getOperatorType();
        switch (type) {
          case QUERY:
          case FILL:
            for (Path p : paths) {
              columns.add(p.getFullPath());
            }
            break;
          case AGGREGATION:
          case GROUPBY:
            List<String> aggregations = plan.getAggregations();
            if (aggregations.size() != paths.size()) {
              for (int i = 1; i < paths.size(); i++) {
                aggregations.add(aggregations.get(0));
              }
            }
            for (int i = 0; i < paths.size(); i++) {
              columns.add(aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")");
            }
            break;
          default:
            throw new TException("not support " + type + " in new read process");
        }
      }

      resp.setOperationType(plan.getOperatorType().toString());
      TSHandleIdentifier operationId = new TSHandleIdentifier(
          ByteBuffer.wrap(username.get().getBytes()),
          ByteBuffer.wrap("PASS".getBytes()));
      TSOperationHandle operationHandle;
      resp.setColumns(columns);
      operationHandle = new TSOperationHandle(operationId, true);
      resp.setOperationHandle(operationHandle);
      recordANewQuery(statement, plan);
      return resp;
    } catch (Exception e) {
      LOGGER.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  protected void checkFileLevelSet(List<Path> paths) throws PathErrorException {
    MManager.getInstance().checkFileLevel(paths);
  }

  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) throws TException {
    try {
      if (!checkLogin()) {
        return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Not login.");
      }
      String statement = req.getStatement();

      if (!queryStatus.get().containsKey(statement)) {
        return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Has not executed statement");
      }

      int fetchSize = req.getFetch_size();
      QueryDataSet queryDataSet;
      if (!queryRet.get().containsKey(statement)) {
        queryDataSet = createNewDataSet(statement, fetchSize, req);
      } else {
        queryDataSet = queryRet.get().get(statement);
      }

      IAuthorizer authorizer;
      try {
        authorizer = LocalFileAuthorizer.getInstance();
      } catch (AuthException e) {
        throw new TException(e);
      }
      TSQueryDataSet result;
      if (authorizer.isUserUseWaterMark(username.get())) {
        result = Utils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize, config);
      } else {
        result = Utils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize);
      }
      boolean hasResultSet = !result.getRecords().isEmpty();
      if (!hasResultSet && queryRet.get() != null) {
        queryRet.get().remove(statement);
      }
      TSFetchResultsResp resp = getTSFetchResultsResp(TS_StatusCode.SUCCESS_STATUS,
          "FetchResult successfully. Has more result: " + hasResultSet);
      resp.setHasResultSet(hasResultSet);
      resp.setQueryDataSet(result);
      return resp;
    } catch (Exception e) {
      LOGGER.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  protected QueryDataSet createNewDataSet(String statement, int fetchSize, TSFetchResultsReq req)
      throws PathErrorException, QueryFilterOptimizationException, FileNodeManagerException,
      ProcessorException, IOException {
    PhysicalPlan physicalPlan = queryStatus.get().get(statement);
    processor.getExecutor().setFetchSize(fetchSize);

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());

    initContextMap();
    contextMapLocal.get().put(req.queryId, context);

    QueryDataSet queryDataSet = processor.getExecutor().processQuery((QueryPlan) physicalPlan,
        context);
    queryRet.get().put(statement, queryDataSet);
    return queryDataSet;
  }

  protected void initContextMap() {
    Map<Long, QueryContext> contextMap = contextMapLocal.get();
    if (contextMap == null) {
      contextMap = new HashMap<>();
      contextMapLocal.set(contextMap);
    }
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req)
      throws TException {
    try {
      if (!checkLogin()) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Not login");
      }
      String statement = req.getStatement();
      return executeUpdateStatement(statement);
    } catch (ProcessorException e) {
      LOGGER.error("meet error while executing update statement.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    } catch (Exception e) {
      LOGGER.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan) {
    List<Path> paths = plan.getPaths();

    try {
      if (!checkAuthorization(paths, plan)) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "No permissions for this operation " + plan.getOperatorType());
      }
    } catch (AuthException e) {
      LOGGER.error("meet error while checking authorization.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Uninitialized authorizer " + e.getMessage());
    }
    // TODO
    // In current version, we only return OK/ERROR
    // Do we need to add extra information of executive condition
    boolean execRet;
    try {
      execRet = executeNonQuery(plan);
    } catch (ProcessorException e) {
      LOGGER.debug("meet error while processing non-query. ", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
    TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
    String msg = execRet ? "Execute successfully" : "Execute statement error.";
    TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle;
    operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  protected boolean executeNonQuery(PhysicalPlan plan) throws ProcessorException {
    return processor.getExecutor().processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement)
      throws ProcessorException {

    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      physicalPlan.setProposer(username.get());
    } catch (QueryProcessorException | ArgsErrorException e) {
      LOGGER.error("meet error while parsing SQL to physical plan!", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    if (physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Statement is a query statement.");
    }

    return executeUpdateStatement(physicalPlan);
  }

  private void recordANewQuery(String statement, PhysicalPlan physicalPlan) {
    queryStatus.get().put(statement, physicalPlan);
    // refresh current queryRet for statement
    if (queryRet.get().containsKey(statement)) {
      queryRet.get().remove(statement);
    }
  }

  /**
   * Check whether current user has logined.
   *
   * @return true: If logined; false: If not logined
   */
  protected boolean checkLogin() {
    return username.get() != null;
  }

  protected boolean checkAuthorization(List<Path> paths, PhysicalPlan plan) throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username.get(), paths, plan.getOperatorType(), targetUser);
  }

  private TSExecuteStatementResp getTSExecuteStatementResp(TS_StatusCode code, String msg) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  protected TSExecuteBatchStatementResp getTSBathExecuteStatementResp(TS_StatusCode code,
      String msg,
      List<Integer> result) {
    TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    resp.setResult(result);
    return resp;
  }

  private TSFetchResultsResp getTSFetchResultsResp(TS_StatusCode code, String msg) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    return resp;
  }

  protected void handleClientExit() throws TException {
    closeOperation(null);
    closeSession(null);
  }

  @Override
  public TSGetTimeZoneResp getTimeZone() throws TException {
    TS_Status tsStatus;
    TSGetTimeZoneResp resp;
    try {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      resp = new TSGetTimeZoneResp(tsStatus, zoneIds.get().toString());
    } catch (Exception e) {
      LOGGER.error("meet error while generating time zone.", e);
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage(e.getMessage());
      resp = new TSGetTimeZoneResp(tsStatus, "Unknown time zone");
    }
    return resp;
  }

  @Override
  public TSSetTimeZoneResp setTimeZone(TSSetTimeZoneReq req) throws TException {
    TS_Status tsStatus;
    try {
      String timeZoneID = req.getTimeZone();
      zoneIds.set(ZoneId.of(timeZoneID));
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      LOGGER.error("meet error while setting time zone.", e);
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage(e.getMessage());
    }
    return new TSSetTimeZoneResp(tsStatus);
  }

  @Override
  public ServerProperties getProperties() throws TException {
    ServerProperties properties = new ServerProperties();
    properties.setVersion(IoTDBConstant.VERSION);
    properties.setSupportedTimeAggregationOperations(new ArrayList<>());
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MAX_TIME);
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MIN_TIME);
    return properties;
  }
}

