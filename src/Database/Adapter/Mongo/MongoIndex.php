<?php

namespace Utopia\Database\Adapter\Mongo;

class MongoIndex {
  public function __construct(
    public string $name, 
    public assoc_array $key,
    public bool $unique = true,
  ) {}
}