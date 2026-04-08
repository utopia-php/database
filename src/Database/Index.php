<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;
use Utopia\Query\Schema\IndexType;

/**
 * Represents a database index with its type, target attributes, and configuration.
 */
class Index
{
    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<string|null>  $orders
     */
    public function __construct(
        public string $key,
        public IndexType $type,
        public array $attributes = [],
        public array $lengths = [],
        public array $orders = [],
        public int $ttl = 1,
    ) {
    }

    /**
     * Convert this index to a Document representation.
     *
     * @return Document
     */
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

    /**
     * Create an Index instance from a Document.
     *
     * @param Document $document The document to convert
     * @return self
     */
    /**
     * Create from an associative array (used by collection config files).
     *
     * @param  array<string, mixed>  $data
     */
    public static function fromArray(array $data): self
    {
        /** @var IndexType|string $type */
        $type = $data['type'] ?? 'key';

        return new self(
            key: $data['$id'] ?? $data['key'] ?? '',
            type: $type instanceof IndexType ? $type : IndexType::from((string) $type),
            attributes: $data['attributes'] ?? [],
            lengths: $data['lengths'] ?? [],
            orders: $data['orders'] ?? [],
            ttl: $data['ttl'] ?? 1,
        );
    }

    public static function fromDocument(Document $document): self
    {
        /** @var string $key */
        $key = $document->getAttribute('key', $document->getId());
        /** @var string $type */
        $type = $document->getAttribute('type', IndexType::Key->value);
        /** @var array<string> $attributes */
        $attributes = $document->getAttribute('attributes', []);
        /** @var array<int> $lengths */
        $lengths = $document->getAttribute('lengths', []);
        /** @var array<string> $orders */
        $orders = $document->getAttribute('orders', []);
        /** @var int $ttl */
        $ttl = $document->getAttribute('ttl', 1);

        return new self(
            key: $key,
            type: IndexType::from($type),
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }
}
