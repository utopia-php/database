<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;
use Utopia\Query\Schema\IndexType;

class Index
{
    public function __construct(
        public string $key,
        public IndexType $type,
        public array $attributes = [],
        public array $lengths = [],
        public array $orders = [],
        public int $ttl = 1,
    ) {
    }

    public function toDocument(): Document
    {
        return new Document([
            '$id' => ID::custom($this->key),
            'key' => $this->key,
            'type' => $this->type->value,
            'attributes' => $this->attributes,
            'lengths' => $this->lengths,
            'orders' => $this->orders,
            'ttl' => $this->ttl,
        ]);
    }

    public static function fromDocument(Document $document): self
    {
        return new self(
            key: $document->getAttribute('key', $document->getId()),
            type: IndexType::from($document->getAttribute('type', 'index')),
            attributes: $document->getAttribute('attributes', []),
            lengths: $document->getAttribute('lengths', []),
            orders: $document->getAttribute('orders', []),
            ttl: $document->getAttribute('ttl', 1),
        );
    }
}
