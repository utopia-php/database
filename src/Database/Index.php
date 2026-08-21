<?php

namespace Utopia\Database;

use Utopia\Database\Helpers\ID;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

/**
 * Represents a database index with its type, target attributes, and configuration.
 *
 * @property string $key
 * @property IndexType $type
 * @property array<string> $attributes
 * @property array<int|null> $lengths
 * @property array<Order|null> $orders
 * @property int $ttl
 */
class Index extends Document
{
    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public function __construct(
        string $key,
        IndexType $type,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ) {
        parent::__construct([
            self::ID => $key,
            'key' => $key,
            'type' => $type->value,
            'attributes' => $attributes,
            'lengths' => $lengths,
            'orders' => self::encodeOrders($orders),
            'ttl' => $ttl,
        ]);
    }

    public function __get(string $name): mixed
    {
        switch ($name) {
            case 'key':
                /** @var string $key */
                $key = $this->getAttribute('key', $this->getId());

                return $key;
            case 'type':
                $type = $this->getAttribute('type', IndexType::Key->value);
                if ($type instanceof IndexType) {
                    return $type;
                }

                return IndexType::from(\is_string($type) ? $type : IndexType::Key->value);
            case 'attributes':
                return $this->getAttribute('attributes', []);
            case 'lengths':
                return $this->getAttribute('lengths', []);
            case 'orders':
                $stored = $this->getAttribute('orders', []);

                return self::decodeOrders(\is_array($stored) ? $stored : []);
            case 'ttl':
                /** @var int $ttl */
                $ttl = $this->getAttribute('ttl', 1);

                return $ttl;
            default:
                return $this->getAttribute($name);
        }
    }

    public function __set(string $name, mixed $value): void
    {
        match ($name) {
            'key' => $this->setAttribute('key', $value)->setAttribute(self::ID, $value),
            'type' => $this->setAttribute('type', $value instanceof IndexType ? $value->value : $value),
            'attributes' => $this->setAttribute('attributes', $value),
            'lengths' => $this->setAttribute('lengths', $value),
            'orders' => $this->setAttribute('orders', self::encodeOrders(\is_array($value) ? $value : [])),
            'ttl' => $this->setAttribute('ttl', $value),
            default => $this->setAttribute($name, $value),
        };
    }

    public function __isset(string $name): bool
    {
        return match ($name) {
            'key', 'type', 'attributes', 'lengths', 'orders', 'ttl' => true,
            default => $this->offsetExists($name),
        };
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function key(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Key,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function index(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Index,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function unique(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Unique,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function fullText(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Fulltext,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function spatial(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Spatial,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function object(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Object,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function hnswEuclidean(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::HnswEuclidean,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function hnswCosine(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::HnswCosine,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function hnswDot(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::HnswDot,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function trigram(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Trigram,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    public static function ttl(
        string $key,
        array $attributes = [],
        array $lengths = [],
        array $orders = [],
        int $ttl = 1,
    ): self {
        return new self(
            key: $key,
            type: IndexType::Ttl,
            attributes: $attributes,
            lengths: $lengths,
            orders: $orders,
            ttl: $ttl,
        );
    }

    /**
     * Convert this index to a Document representation.
     *
     * @return Document
     */
    public function toDocument(): Document
    {
        return new Document([
            Document::ID => ID::custom($this->key),
            'key' => $this->key,
            'type' => $this->type->value,
            'attributes' => $this->attributes,
            'lengths' => $this->lengths,
            'orders' => $this->getAttribute('orders', []),
            'ttl' => $this->ttl,
        ]);
    }

    /**
     * Create from an associative array (used by collection config files).
     *
     * @param  array<string, mixed>  $data
     */
    public static function fromArray(array $data): self
    {
        /** @var IndexType|string $type */
        $type = $data['type'] ?? 'key';
        /** @var string $key */
        $key = $data[Document::ID] ?? $data['key'] ?? '';
        /** @var array<string> $attributes */
        $attributes = $data['attributes'] ?? [];
        /** @var array<int|null> $lengths */
        $lengths = $data['lengths'] ?? [];
        /** @var array<mixed> $orders */
        $orders = $data['orders'] ?? [];
        /** @var int $ttl */
        $ttl = $data['ttl'] ?? 1;

        return self::make(
            key: $key,
            type: $type instanceof IndexType ? $type : IndexType::from((string) $type),
            attributes: $attributes,
            lengths: $lengths,
            orders: self::decodeOrders($orders),
            ttl: $ttl,
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
        /** @var array<mixed> $orders */
        $orders = $document->getAttribute('orders', []);
        /** @var int $ttl */
        $ttl = $document->getAttribute('ttl', 1);

        return self::make(
            key: $key,
            type: IndexType::from($type),
            attributes: $attributes,
            lengths: $lengths,
            orders: self::decodeOrders($orders),
            ttl: $ttl,
        );
    }

    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<Order|null>  $orders
     */
    private static function make(
        string $key,
        IndexType $type,
        array $attributes,
        array $lengths,
        array $orders,
        int $ttl,
    ): self {
        return match ($type) {
            IndexType::Key => self::key(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Index => self::index(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Unique => self::unique(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Fulltext => self::fullText(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Spatial => self::spatial(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Object => self::object(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::HnswEuclidean => self::hnswEuclidean(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::HnswCosine => self::hnswCosine(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::HnswDot => self::hnswDot(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Trigram => self::trigram(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
            IndexType::Ttl => self::ttl(
                key: $key,
                attributes: $attributes,
                lengths: $lengths,
                orders: $orders,
                ttl: $ttl,
            ),
        };
    }

    public static function direction(?Order $order): string
    {
        return $order === null ? '' : $order->value;
    }

    /**
     * @param  array<mixed>  $orders
     * @return array<string|null>
     */
    private static function encodeOrders(array $orders): array
    {
        $encoded = [];

        foreach ($orders as $order) {
            if ($order instanceof Order) {
                $encoded[] = $order->value;
                continue;
            }

            if ($order === null) {
                $encoded[] = null;
                continue;
            }

            throw new \InvalidArgumentException('Index order must be Order or null');
        }

        return $encoded;
    }

    /**
     * @param  array<mixed>  $orders
     * @return array<Order|null>
     */
    private static function decodeOrders(array $orders): array
    {
        $decoded = [];

        foreach ($orders as $order) {
            if ($order instanceof Order || $order === null) {
                $decoded[] = $order;
                continue;
            }

            if (\is_string($order)) {
                $decoded[] = Order::from($order);
                continue;
            }

            throw new \InvalidArgumentException('Index order must be Order or null');
        }

        return $decoded;
    }
}
