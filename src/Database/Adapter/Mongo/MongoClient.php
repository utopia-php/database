<?php

namespace Utopia\Database\Adapter\Mongo;

use MongoDB\BSON;

use Utopia\Database\Adapter\Mongo\Auth;
use Utopia\Database\Adapter\Mongo\Command;
use Utopia\Database\Adapter\Mongo\MongoClientOptions;
use Utopia\Database\Database;
use Utopia\Database\Adapter\Mongo\MongoIndex;
use Utopia\Database\Query;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;

class MongoClient 
{ 
  /**
   * Unique identifier for socket connection.
   */
  private $id;

  /**
   * Options for connection.
   */
  private $options;

  /**
   * Socket client.
   */
  private $client;

  /**
   * Authentication for connection
   */
  private $auth;

  /**
   * Create a Mongo connection.
   * @param MongoClientOptions $options
   * @param Boolean $useCoroutine
   */
  public function __construct(MongoClientOptions $options, $useCoroutine = true) {
    $this->id = uniqid('utopia.mongo.client');
    $this->options = $options;

    $this->client = $useCoroutine
    ? new \Swoole\Coroutine\Client(SWOOLE_SOCK_TCP | SWOOLE_KEEP)
    : new \Swoole\Client(SWOOLE_SOCK_TCP | SWOOLE_KEEP);

    $this->auth = new Auth([
      'authcid' => $options->username,
      'secret' => Auth::encodeCredentials($options->username, $options->password)
    ]);
  }

  /**
   * Connect to Mongo using TCP/IP 
   * and Wire Protocol.
   */
  public function connect() {
    $this->client->connect($this->options->host, $this->options->port);
    [$payload, $db] = $this->auth->start();

    $res = $this->query($payload, $db);

    [$payload, $db] = $this->auth->continue($res);
    
    $res = $this->query($payload, $db);

    return $this;
  }

  /**
   * Send a raw string query to connection.
   */
  public function raw_query($string) {
    return $this->send($string);
  }

  /**
   * Send a BSON packed query to connection.
   * 
   * @param array $command
   * @param string $db
   */
  public function query(array $command, $db = null) {
    $params = array_merge($command, [
      '$db' => $db ?? $this->options->name,
    ]);

    $sections = BSON\fromPHP($params);
    $message = pack('V*', 21 + strlen($sections), $this->id, 0, 2013, 0) . "\0" . $sections;

    return $this->send($message);
  }

  /**
   * Send a syncronous command to connection.
   */
  public function blocking($cmd) {
    $this->client->send($cmd . PHP_EOL);

    $result = '';

    while(true) {
      $data = $this->client->recv();

      Co::sleep(0.5);
    }

    return $result;
  }

  /**
   * Send a message to connection.
   * 
   * @param $data
   */
  public function send($data) {
    $this->client->send($data);

    return $this->receive();
  }

  /**
   * Receive a message from connection.
   */
  private function receive() {
    $receivedLength = 0;
    $responseLength = null;
    $res = '';

    do {
      if (($chunk = $this->client->recv()) === false) {
          \Co::sleep(1); // Prevent excessive CPU Load, test lower.
          continue;
      }
      
      $receivedLength += strlen($chunk);
      $res .= $chunk;

      if ((!isset($responseLength)) && (strlen($res) >= 4)) {
          $responseLength = unpack('Vl', substr($res, 0, 4))['l'];
      }

    } while (
      (!isset($responseLength)) || ($receivedLength < $responseLength) 
    );

    $result = BSON\toPHP(substr($res, 21, $responseLength - 21));

    $cnt = -1;

    if(property_exists($result, "writeErrors")) {
      throw new DuplicateException($result->writeErrors[0]->errmsg);
    }

    if(property_exists($result, "n") && $result->ok == 1) {
      return $result->n;
    }

    if(property_exists($result, "nonce") && $result->ok == 1) {
      return $result;
    }


    if(property_exists($result, 'errmsg')) {
      throw new \Exception($result->errmsg);
    }

    if($result->ok == 1) {
      return $result;
    }

    return $result->cursor->firstBatch;
  }
  
  /**
   * Selects a collection.
   * 
   * Note: Since Mongo creates on the fly, this just returns
   * a instances of self.
   */
  public function selectDatabase($name) {
    return $this;
  }

  /**
   * Creates a collection.
   * 
   * Note: Since Mongo creates on the fly, this just returns
   * a instances of self.
   */
  public function createDatabase($name) {
    return $this;
  }

  /**
   * Get a list of databases.
   */
  public function listDatabaseNames() {
    return $this->query([
      'listDatabases' => 1,
      'nameOnly' => true,
    ], 'admin');
  }

  /**
   * Drop (remove) a database.
   * 
   * @param array $options
   * @param string $db
   */
  // https://docs.mongodb.com/manual/reference/command/dropDatabase/#mongodb-dbcommand-dbcmd.dropDatabase
  public function dropDatabase(array $options = [], string $db = null) {
    $db = $db ?? $this->options->name;

    $this->query(array_merge(["dropDatabase" => 1], $options), $db);

    return $this;
  }

  public function selectCollection($name) {
    return $this;
  }

  /**
   * Create a collection.
   * 
   * @param string $name
   * @param array $options
   */
  // For options see: https://docs.mongodb.com/manual/reference/command/create/#mongodb-dbcommand-dbcmd.create
  public function createCollection($name, $options = []) {
    $list = $this->listCollectionNames(["name" => $name]);

    if(\count($list->cursor->firstBatch) > 0) {
      return $this;
    }

    $result = $this->query(array_merge([
      'create' => $name,
    ], $options));

    return $this;
  }

  /**
   * Drop a collection.
   * 
   * @param string $name
   * @param array $options
   */
  // https://docs.mongodb.com/manual/reference/command/drop/#mongodb-dbcommand-dbcmd.drop
  public function dropCollection(string $name, $options = []) {
    $this->query(array_merge([
      'drop' => $name,
    ], $options));

    return $this;
  }

  /**
   * List collections (name only).
   * 
   * @param string $name
   * @param array $options
   * 
   * @return array
   */

  // https://docs.mongodb.com/manual/reference/command/listCollections/#listcollections
  public function listCollectionNames($filter = [], $options = []) {
    $qry = array_merge([
      "listCollections" => 1.0,
      "nameOnly" => true,
      "authorizedCollections" => true,
      "filter" => $this->toObject($filter)],
      $options
    );

    return $this->query($qry);
  }

  /**
   * Create indexes.
   * 
   * @param string $collection
   * @param array $indexes
   * @param array $options
   * 
   * @return array
   */

  // https://docs.mongodb.com/manual/reference/command/createIndexes/#createindexes
  public function createIndexes($collection, $indexes, $options = []) {

    $qry = array_merge(
      [
        'createIndexes' => $collection, 
        'indexes' => $indexes
      ], $options);

    $this->query($qry);

    return true;
  }

  /**
   * Drop indexes from a collection.
   * 
   * @param string $collection
   * @param array $indexes
   * @param array $options
   * 
   * @return array
   */
  // https://docs.mongodb.com/manual/reference/command/dropIndexes/#dropindexes
  public function dropIndexes(string $collection, $indexes, $options = []) {
    $this->query(array_merge([
        'dropIndexes' => $collection,
        'index' => $indexes,
      ], $options)
    );

    return $this;
  }

  /**
   * Insert a document/s.
   * 
   * @param string $collection
   * @param array $documents
   * @param array $options
   * 
   * @return Document
   */

  // https://docs.mongodb.com/manual/reference/command/insert/#mongodb-dbcommand-dbcmd.insert
  public function insert(string $collection, $documents, $options = []) {
    $docObj = new \stdClass();

    foreach($documents as $key => $value) {
        $docObj->{$key} = $value;
    }

    $result = $this->query(array_merge([
      MongoCommand::INSERT => $collection, 
      'documents' => [$docObj],
    ], $options));

    return new Document($this->toArray($docObj));
  }

  /**
   * Update a document/s.
   * 
   * @param string $collection
   * @param array $documents
   * @param array $options
   * 
   * @return MongoClient
   */

  // https://docs.mongodb.com/manual/reference/command/update/#syntax
  public function update(string $collection, $where = [], $updates = [], $options = []) {

    $this->query(array_merge([
      MongoCommand::UPDATE => $collection, 
      'updates' => [
          [
            "q" => $this->toObject($where),
            "u" => $this->toObject($updates),
            'multi' => false,
            'upsert' => false
          ]
        ]
      ], $options)
    );

    return $this;
  }

  /**
   * Insert, or update, a document/s.
   * 
   * @param string $collection
   * @param array $documents
   * @param array $options
   * 
   * @return MongoClient
   */


  // https://docs.mongodb.com/manual/reference/command/update/#syntax
  public function upsert(string $collection, $where = [], $updates = [], $options = []) {
   $this->query(array_merge(
     [
       "update" => $collection,
       "updates" => [
         [
           "q" => ["_uid" => $where["_uid"]],
           "u" => ["\$set" => $updates],
         ]
        ],
      ], $options)
   );

    return $this;
  }

  /**
   * Find a document/s.
   * 
   * @param string $collection
   * @param array $filters
   * @param array $options
   * 
   * @return array
   */
  // https://docs.mongodb.com/manual/reference/command/find/#mongodb-dbcommand-dbcmd.find
  public function find(string $collection, $filters = [], $options = []) {
    
    $result =  $this->query(array_merge([
      MongoCommand::FIND => $collection,
      'filter' => $this->toObject($filters),
      ], $options)
    );

    return $result;
  }

  /**
   * Find and modify document/s.
   * 
   * @param string $collection
   * @param array $filters
   * @param array $options
   * 
   * @return array
   */
  
  // https://docs.mongodb.com/manual/reference/command/findAndModify/#mongodb-dbcommand-dbcmd.findAndModify
  public function findAndModify(string $collection, $document, $update, $remove = false, $filters = [], $options = []) {
    return $this->query(array_merge([
      MongoCommand::FIND_AND_MODIFY => $collection,
      'filter' => $this->toObject($filters),
      'remove' => $remove,
      'update' => $update,
      ], $options)
    );
  }

  /**
   * Delete a document/s.
   * 
   * @param string $collection
   * @param array $filters
   * @param array $options
   * 
   * @return array
   */
  
  // https://docs.mongodb.com/manual/reference/command/delete/#mongodb-dbcommand-dbcmd.delete
  public function delete(string $collection, $filters = [], $limit = 1, $deleteOptions = [], $options = []) {
    return $this->query(array_merge([
      MongoCommand::DELETE => $collection,
      'deletes' => [
        $this->toObject(array_merge(
          [
            'q' => $this->toObject($filters),
            'limit' => $limit,
          ], $deleteOptions)
        ),
      ]],
      $options)
    );
  }

  /**
   * Count documents.
   * 
   * @param string $collection
   * @param array $filters
   * @param array $options
   * 
   * @return array
   */
  
  public function count($collection, $filters, $options) {
    $result = $this->find($collection, $filters, $options);
    $list = $result->cursor->firstBatch;
    
    return \count($list);
  }

  /**
   * Aggregate a collection pipeline.
   * 
   * @param string $collection
   * @param array $pipeline
   
   * 
   * @return array
   */
  
  public function aggregate($collection, $pipeline) {
    $result = $this->query([
      MongoCommand::AGGREGATE => $collection,
      'pipeline' => $pipeline,
      'cursor' => $this->toObject([]),
    ]);

    return $result;
  }

  /**
   * Convert an assoc array to an object (stdClass).
   * 
   * @param array $dict
   * 
   * @return stdClass
   */
  
  public function toObject($dict) {
    $obj = new \stdClass();

    foreach($dict as $k => $v) {
      $key = $k == 'id' ? '_id' : $k;
      $val = $v;

      if($k == '_id') {
        $val = new \MongoDB\BSON\ObjectId($v);
      }

      $obj->{$key} = $val;
    }

    return $obj;
  }

  /**
   * Convert an object (stdClass) to an assoc array.
   * 
   * @param string $obj
   * 
   * @return array
   */
  
  public function toArray($obj) {
    if(is_object($obj) || is_array($obj)) {
      $ret = (array) $obj;
      foreach($ret as $item) {
          $item = $this->toArray($item);
      }

      return $ret;
    } else {
      return $obj;
    }
  }
}
