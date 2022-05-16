<?php

namespace Utopia\Database\Adapter\Mongo;

class MongoCommand {
  /**
   * Defines commands Mongo uses over wire protocol.
   */
  const CREATE = "create";
  const DELETE = "delete";
  const FIND = "find";
  const FIND_AND_MODIFY = "findAndModify";
  const GET_LAST_ERROR = "getLastError";
  const GET_MORE = "getMore";
  const INSERT = "insert";
  const RESET_ERROR = "resetError";
  const UPDATE = "update";
  const COUNT = "count";
  const AGGREGATE = "aggregate";
  const DISTINCT = "distinct";
  const MAP_REDUCE = "mapReduce";
}
