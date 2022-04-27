<?php

namespace Utopia\Database\Adapter\Mongo;

class MongoIndex {
  public function __construct(
    public string $name, 
    public array $key,
    public bool $unique = true,
    public array $options = []
  ) {}

  public function toQuery() {
    return array_merge([
      'name' => $this->name,
      'key' => $this->key,
      'unique' => $this->unique,
    ], $this->options);
  }
}