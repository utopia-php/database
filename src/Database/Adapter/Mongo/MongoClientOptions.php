<?php

namespace Utopia\Database\Adapter\Mongo;

class MongoClientOptions
{
  /**
   * Creates an object that contains the connection
   * details for Mongo.
   */
  public function __construct(
    public string $name,
    public string $host,
    public int $port,
    public string $username,
    public string $password
  ){}
}
