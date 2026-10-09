<?php

namespace Utopia\Database;

use Utopia\Database\Exception\Index as IndexException;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\IndexType;

final readonly class Index
{
    private const string KEY = 'key';

    private const string TYPE = 'type';

    private const string ATTRIBUTES = 'attributes';

    private const string LENGTHS = 'lengths';

    private const string ORDERS = 'orders';

    private const string TTL = 'ttl';

    /**
     * @param  list<string>  $attributes
     * @param  list<?int>  $lengths
     * @param  list<?OrderDirection>  $orders
     */
    private function __construct(
        public string $key,
        public IndexType $type,
        public array $attributes,
        public array $lengths,
        public array $orders,
        public ?int $ttl,
    ) {
    }

    /**
     * @param  list<string>  $attributes
     * @param  list<?int>  $lengths
     * @param  list<?OrderDirection>  $orders
     *
     * @throws IndexException
     */
    public static function key(string $key, array $attributes, array $lengths = [], array $orders = []): self
    {
        return self::make($key, IndexType::Key, $attributes, $lengths, $orders, null);
    }

    /**
     * @param  list<string>  $attributes
     * @param  list<?int>  $lengths
     * @param  list<?OrderDirection>  $orders
     *
     * @throws IndexException
     */
    public static function unique(string $key, array $attributes, array $lengths = [], array $orders = []): self
    {
        return self::make($key, IndexType::Unique, $attributes, $lengths, $orders, null);
    }

    /**
     * @param  list<string>  $attributes
     *
     * @throws IndexException
     */
    public static function fulltext(string $key, array $attributes): self
    {
        return self::make($key, IndexType::Fulltext, $attributes, [], [], null);
    }

    /**
     * @param  list<string>  $attributes
     *
     * @throws IndexException
     */
    public static function trigram(string $key, array $attributes): self
    {
        return self::make($key, IndexType::Trigram, $attributes, [], [], null);
    }

    /**
     * @throws IndexException
     */
    public static function spatial(string $key, string $attribute, ?OrderDirection $order = null): self
    {
        return self::make($key, IndexType::Spatial, [$attribute], [], $order === null ? [] : [$order], null);
    }

    /**
     * @throws IndexException
     */
    public static function object(string $key, string $attribute): self
    {
        return self::make($key, IndexType::Object, [$attribute], [], [], null);
    }

    /**
     * @throws IndexException
     */
    public static function hnswEuclidean(string $key, string $attribute): self
    {
        return self::make($key, IndexType::HnswEuclidean, [$attribute], [], [], null);
    }

    /**
     * @throws IndexException
     */
    public static function hnswCosine(string $key, string $attribute): self
    {
        return self::make($key, IndexType::HnswCosine, [$attribute], [], [], null);
    }

    /**
     * @throws IndexException
     */
    public static function hnswDot(string $key, string $attribute): self
    {
        return self::make($key, IndexType::HnswDot, [$attribute], [], [], null);
    }

    /**
     * @throws IndexException
     */
    public static function ttl(string $key, string $attribute, int $ttl): self
    {
        return self::make($key, IndexType::Ttl, [$attribute], [], [], $ttl);
    }

    /**
     * @throws IndexException
     */
    public static function fromDocument(Document $document): self
    {
        return self::hydrate(
            $document->getAttribute(self::KEY, $document->getId()),
            $document->getAttribute(self::TYPE, IndexType::Key->value),
            $document->getAttribute(self::ATTRIBUTES, []),
            $document->getAttribute(self::LENGTHS, []),
            $document->getAttribute(self::ORDERS, []),
            $document->getAttribute(self::TTL),
        );
    }

    /**
     * @param  array<string, mixed>  $data
     *
     * @throws IndexException
     */
    public static function fromArray(array $data): self
    {
        $type = $data[self::TYPE] ?? null;
        if (\is_string($type) && IndexType::tryFrom($type) === null) {
            throw new IndexException(self::unknownTypeMessage($type));
        }

        return self::hydrate(
            $data[self::KEY] ?? $data[Document::ID] ?? '',
            $data[self::TYPE] ?? IndexType::Key->value,
            $data[self::ATTRIBUTES] ?? [],
            $data[self::LENGTHS] ?? [],
            $data[self::ORDERS] ?? [],
            $data[self::TTL] ?? null,
        );
    }

    /**
     * @internal
     */
    public static function unknownTypeMessage(string $type): string
    {
        return 'Unknown index type: '.$type.'. Must be one of '.IndexType::Key->value.', '.IndexType::Unique->value.', '.IndexType::Fulltext->value.', '.IndexType::Spatial->value.', '.IndexType::Object->value.', '.IndexType::HnswEuclidean->value.', '.IndexType::HnswCosine->value.', '.IndexType::HnswDot->value.', '.IndexType::Trigram->value.', '.IndexType::Ttl->value;
    }

    public function toDocument(): Document
    {
        $data = [
            Document::ID => $this->key,
            self::KEY => $this->key,
            self::TYPE => $this->type->value,
            self::ATTRIBUTES => $this->attributes,
        ];

        if (self::storesLengths($this->type)) {
            $data[self::LENGTHS] = $this->lengths;
        }

        if (self::storesOrders($this->type)) {
            $data[self::ORDERS] = \array_map(
                static fn (?OrderDirection $order): ?string => $order?->value,
                $this->orders,
            );
        }

        if ($this->type === IndexType::Ttl) {
            $data[self::TTL] = $this->ttl;
        }

        return new Document($data);
    }

    public function withKey(string $key): self
    {
        return clone($this, ['key' => $key]);
    }

    /**
     * @param  list<?int>  $lengths
     *
     * @throws IndexException
     */
    public function withLengths(array $lengths): self
    {
        if ($lengths !== [] && ! self::storesLengths($this->type)) {
            throw new IndexException('A '.$this->type->value.' index does not take lengths');
        }

        return clone($this, ['lengths' => self::lengths($lengths)]);
    }

    /**
     * @param  list<?OrderDirection>  $orders
     *
     * @throws IndexException
     */
    public function withOrders(array $orders): self
    {
        if ($orders !== [] && ! self::storesOrders($this->type)) {
            throw new IndexException('A '.$this->type->value.' index does not take orders');
        }

        return clone($this, ['orders' => self::orders($orders)]);
    }

    private static function storesLengths(IndexType $type): bool
    {
        return $type !== IndexType::Fulltext;
    }

    private static function storesOrders(IndexType $type): bool
    {
        return $type !== IndexType::Fulltext && $type !== IndexType::Ttl;
    }

    /**
     * @param  array<mixed>  $attributes
     * @param  array<mixed>  $lengths
     * @param  array<mixed>  $orders
     *
     * @throws IndexException
     */
    private static function make(string $key, IndexType $type, array $attributes, array $lengths, array $orders, mixed $ttl): self
    {
        if ($type === IndexType::Index) {
            $type = IndexType::Key;
        }

        if ($type === IndexType::Ttl && (! \is_int($ttl) || $ttl < 1)) {
            throw new IndexException('TTL must be at least 1 second');
        }

        return new self(
            key: $key,
            type: $type,
            attributes: self::attributes($attributes),
            lengths: self::storesLengths($type) ? self::lengths($lengths) : [],
            orders: self::storesOrders($type) ? self::orders($orders) : [],
            ttl: $type === IndexType::Ttl && \is_int($ttl) ? $ttl : null,
        );
    }

    /**
     * @throws IndexException
     */
    private static function hydrate(mixed $key, mixed $type, mixed $attributes, mixed $lengths, mixed $orders, mixed $ttl): self
    {
        if (! \is_string($key)) {
            throw new IndexException('Index key must be a string');
        }

        $type = $type instanceof IndexType ? $type : IndexType::tryFrom(\is_string($type) ? $type : '') ?? IndexType::Key;

        return self::make(
            $key,
            $type,
            \is_array($attributes) ? $attributes : [],
            \is_array($lengths) ? \array_map(self::storedLength(...), $lengths) : [],
            \is_array($orders) ? \array_map(self::storedOrder(...), $orders) : [],
            $type === IndexType::Ttl ? self::storedLength($ttl) : null,
        );
    }

    private static function storedLength(mixed $length): mixed
    {
        return \is_string($length) && \ctype_digit($length) ? (int) $length : $length;
    }

    /**
     * @throws IndexException
     */
    private static function storedOrder(mixed $order): mixed
    {
        if ($order instanceof OrderDirection || $order === null) {
            return $order;
        }

        if ($order instanceof \BackedEnum) {
            $order = $order->value;
        }

        if ($order === '') {
            return null;
        }

        if (! \is_string($order)) {
            return $order;
        }

        return OrderDirection::tryFrom(\strtoupper($order))
            ?? throw new IndexException('Unknown index order "'.$order.'"');
    }

    /**
     * @param  array<mixed>  $attributes
     * @return list<string>
     *
     * @throws IndexException
     */
    private static function attributes(array $attributes): array
    {
        $list = [];
        foreach ($attributes as $attribute) {
            if (! \is_string($attribute)) {
                throw new IndexException('Index attributes must be strings');
            }
            $list[] = $attribute;
        }

        return $list;
    }

    /**
     * @param  array<mixed>  $lengths
     * @return list<?int>
     *
     * @throws IndexException
     */
    private static function lengths(array $lengths): array
    {
        $list = [];
        foreach ($lengths as $length) {
            if ($length !== null && ! \is_int($length)) {
                throw new IndexException('Index lengths must be integers or null');
            }
            $list[] = $length;
        }

        return $list;
    }

    /**
     * @param  array<mixed>  $orders
     * @return list<?OrderDirection>
     *
     * @throws IndexException
     */
    private static function orders(array $orders): array
    {
        $list = [];
        foreach ($orders as $order) {
            $list[] = match (true) {
                $order === OrderDirection::Random => throw new IndexException('Index orders cannot be random'),
                $order === null, $order instanceof OrderDirection => $order,
                default => throw new IndexException('Index orders must be OrderDirection cases or null'),
            };
        }

        return $list;
    }
}
