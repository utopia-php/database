<?php

namespace Utopia\Database\Adapter\Mongo;

use Utopia\Database\Adapter\Mongo\Auth;
use Utopia\Database\Adapter\Mongo\Command;


class MongoClientOptions
{
  public function __construct(
    public readonly string $name,
    public readonly string $host,
    public readonly int $port,
    public readonly string $username,
    public readonly string $password
  ){}
}

class MongoClient 
{ 
  private $id;
  private $options;
  private $client;
  private $auth;

  public function __construct(MongoClientOptions $options) {
    $this->id = uniqid('utopia.mongo.client');
    $this->options = $options;

    $this->client = new Swoole\Coroutine\Client(SWOOLE_SOCK_TCP | SWOOLE_KEEP);
    $this->auth = new Auth([
      'authcid' => $options->username,
      'secret' => Auth::encodeCredentials($options->username, $options->password)
    ]);
  }

  public function connect() {
    $res = $this->client->connect($this->options->host, $this->options->port);

    return $this;
  }

  public function raw_query($string) {
    return $this->send($string);
  }

  public function query($command, $db = null) {
    $sections = BSON\fromPHP([
      ...$command,
      '$db' => $db ?? $this->options->name,
    ]);

    $message = pack('V*', 21 + strlen($sections), $this->id, 0, 2013, 0) . "\0" . $sections;

    return $this->send($message);
  }

  public function blocking($cmd) {
    $this->client->send($cmd . PHP_EOL);

    $result = '';

    while(true) {
      $data = $this->client->recv();

      Co::sleep(0.5);
    }

    return $result;
  }

  public function send($data) {
    $this->client->send($data);

    return $this->receive();
  }

  private function receive() {
    $receivedLength = 0;
    $responseLength = null;
    $res = '';

    do {
      if (($chunk = $this->client->recv()) === false) {
          Co::sleep(1); // Prevent excessive CPU Load, test lower.
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

    if(property_exists($result, "n") && $result->ok == 1) {
      return "ok";
    }

    if(property_exists($result, "nonce") && $result->ok == 1) {
      return $result;
    }

    if(property_exists($result, 'errmsg')) {
      throw new Exception($result->errmsg);
    }

    if($result->ok == 1) {
      return $result;
    }
    
    return $result->cursor->firstBatch;
  }
  
  public function selectDatabase($name) {
    return $this;
  }

  public function createDatabase($name) {
    return $this;
  }

  public function listDatabaseNames() {
    return $this->query([
      'listDatabases' => 1,
      'nameOnly' => true,
    ]);
  }

  // https://docs.mongodb.com/manual/reference/command/dropDatabase/#mongodb-dbcommand-dbcmd.dropDatabase
  public function dropDatabase($options = []) {
    $this->query([
      "dropDatabase" => 1,
      ...$options
    ]);

    return $this;
  }

  // For options see: https://docs.mongodb.com/manual/reference/command/create/#mongodb-dbcommand-dbcmd.create
  public function createCollection($name, $options = []) {
    $this->query($name, [
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/drop/#mongodb-dbcommand-dbcmd.drop
  public function dropCollection($name, $options = []) {
    $this->query($name, [
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/listCollections/#listcollections
  public function listCollectionNames($filter = [], $options = []) {
    return $this->query([
      "listCollections" => 1,
      "nameOnly" => true,
      "filter" => $this->toObject($filter),
      ...$options
    ]);
  }

  // https://docs.mongodb.com/manual/reference/command/createIndexes/#createindexes
  public function createIndexes($collection, $indexes, $options = []) {
    $this->query([
      'createIndexes' => $collection,
      'indexes' => $indexes,
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/dropIndexes/#dropindexes
  public function dropIndexes($collection, $indexes, $options = []) {
    $this->query([
      'dropIndexes' => $collection,
      'indexes' => $indexes,
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/insert/#mongodb-dbcommand-dbcmd.insert
  public function insert($collection, $documents, $options = []) {
    $documents = is_array($documents) ? $documents : [$documents];
    
    $docObjects = [];
    foreach($documents as $doc) {
      foreach((object)$doc as $k=>$value) {
        $docObj = new \stdClass();
        $docObj->{$k} = $value;

        $docObjects[] = $docObj;
      }
    }

    $this->query([
      MongoCommand::INSERT => $collection, 
      'documents' => $docObjects, 
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/update/#syntax
  public function update($collection, $where = [], $updates = [], $options = []) {
    
    $this->query([
      MongoCommand::UPDATE => $collection, 
      'updates' => [
        [
          'q' => $this->toObject($where),
          'u' => $this->toObject($updates),
          'multi' => false,
          'upsert' => false
        ]
      ],
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/update/#syntax
  public function upsert($collection, $where = [], $updates = [], $options = []) {
    
    $this->query([
      MongoCommand::UPDATE => $collection, 
      'updates' => [
        [
          'q' => $this->toObject($where),
          'u' => $this->toObject($updates),
          'multi' => false,
          'upsert' => true
        ]
      ],
      ...$options
    ]);

    return $this;
  }

  // https://docs.mongodb.com/manual/reference/command/find/#mongodb-dbcommand-dbcmd.find
  public function find($collection, $filters = [], $options = []) {
    return $this->query([
      MongoCommand::FIND => $collection,
      'filter' => $this->toObject($filters),
      ...$options,
    ]);
  }

  // https://docs.mongodb.com/manual/reference/command/findAndModify/#mongodb-dbcommand-dbcmd.findAndModify
  public function findAndModify($collection, $document, $update, $remove = false, $filters = [], $options = []) {
    return $this->query([
      MongoCommand::FIND_AND_MODIFY => $collection,
      'filter' => $this->toObject($filters),
      'remove' => $remove,
      'update' => $update,
      ...$options,
    ]);
  }

  // https://docs.mongodb.com/manual/reference/command/delete/#mongodb-dbcommand-dbcmd.delete
  public function delete($collection, $filters = [], $limit = 1, $deleteOptions = [], $options = []) {
    return $this->query([
      MongoCommand::DELETE => $collection,
      'deletes' => [
        $this->toObject(
          [
            'q' => $this->toObject($filters),
            'limit' => $limit,
            ...$deleteOptions
          ]
        ),
      ],
      ...$options,
    ]);
  }


  private function toObject($dict) {
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

  private function authenticate()
  {
    $this->query(['hello' => 1]);   

    $payload = $this->auth->createResponse();
    $payload = new \MongoDB\BSON\Binary($payload, 0);

    $result = $this->query([
      "saslStart" => 1,
      "mechanism" => "SCRAM-SHA-1",
      "payload" => $payload,
      "autoAuthorize" => 1,
      "options" => ["skipEmptyExchange" => true],
    ], 'admin');  

    $cid = $result->conversationId;
    $token = $result->payload->getData();

    $payload = $auth->createResponse($token);
    $payload = new \MongoDB\BSON\Binary($payload, 0);

    $result = $this->query([
      "saslContinue" => 1,
      "conversationId" => $cid,
      "payload" => $payload,
      "options" => ["skipEmptyExchange" => true],
    ], 'admin');  

    $this->authToken = $res->payload->getData();
  }
}
