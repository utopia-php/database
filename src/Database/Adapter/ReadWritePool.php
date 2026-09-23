<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Pools\Pool as UtopiaPool;

class ReadWritePool extends Pool
{
    private const READ_METHODS = [
        'find',
        'getDocument',
        'count',
        'sum',
        'exists',
        'list',
        'getSchemaAttributes',
        'getSchemaIndexes',
        'getBuilder',
        'getSchema',
        'getColumnType',
        'decodePoint',
        'decodeLinestring',
        'decodePolygon',
        'getSizeOfCollection',
        'getSizeOfCollectionOnDisk',
        'ping',
        'getConnectionId',
        'getDocumentSizeLimit',
        'getAttributeWidth',
        'getCountOfAttributes',
        'getCountOfIndexes',
        'getCountOfDefaultAttributes',
        'getCountOfDefaultIndexes',
        'getLimitForString',
        'getLimitForInt',
        'getLimitForBigInt',
        'getLimitForAttributes',
        'getLimitForIndexes',
        'getMaxIndexLength',
        'getMaxVarcharLength',
        'getMaxUIDLength',
        'getMinDateTime',
        'getIdAttributeType',
        'getKeywords',
        'getInternalIndexesKeys',
        'supports',
        'capabilities',
        'hasFeature',
    ];

    /**
     * @var UtopiaPool<covariant Adapter>
     */
    private UtopiaPool $readPool;

    private bool $sticky = true;

    private int $stickyDurationMs = 5000;

    private ?float $lastWriteTimestamp = null;

    /**
     * @param  UtopiaPool<covariant Adapter>  $writePool
     * @param  UtopiaPool<covariant Adapter>  $readPool
     */
    public function __construct(UtopiaPool $writePool, UtopiaPool $readPool)
    {
        parent::__construct($writePool);
        $this->readPool = $readPool;
    }

    public function setStickyDuration(int $milliseconds): static
    {
        $this->stickyDurationMs = $milliseconds;

        return $this;
    }

    public function setSticky(bool $sticky): static
    {
        $this->sticky = $sticky;

        return $this;
    }

    public function delegate(string $method, array $args): mixed
    {
        return $this->borrowAndInvoke($method, $args);
    }

    #[\Override]
    public function withTransaction(callable $callback): mixed
    {
        try {
            return parent::withTransaction($callback);
        } finally {
            $this->stick();
        }
    }

    /**
     * @param  array<mixed>  $args
     * @param  class-string|null  $feature
     */
    #[\Override]
    protected function borrowAndInvoke(string $method, array $args, ?string $feature = null): mixed
    {
        if (! $this->isReadOperation($method, $args)) {
            try {
                return parent::borrowAndInvoke($method, $args, $feature);
            } finally {
                $this->stick();
            }
        }

        if ($this->pin() !== null || $this->isSticky()) {
            return parent::borrowAndInvoke($method, $args, $feature);
        }

        return $this->readPool->use(function (Adapter $adapter) use ($method, $args, $feature) {
            try {
                $this->syncBorrowedAdapter($adapter);

                return $this->invokeDelegated($adapter, $method, $args, $feature);
            } finally {
                $this->releaseBorrowedAdapter($adapter);
            }
        });
    }

    /**
     * @param  array<mixed>  $args
     */
    private function isReadOperation(string $method, array $args): bool
    {
        return \in_array($method, self::READ_METHODS, true) && ! $this->locksRow($method, $args);
    }

    /**
     * @param  array<mixed>  $args
     */
    private function locksRow(string $method, array $args): bool
    {
        return $method === 'getDocument' && ($args[3] ?? $args['forUpdate'] ?? false) === true;
    }

    private function stick(): void
    {
        $this->lastWriteTimestamp = \microtime(true);
    }

    private function isSticky(): bool
    {
        if (! $this->sticky || $this->lastWriteTimestamp === null) {
            return false;
        }

        $elapsed = (\microtime(true) - $this->lastWriteTimestamp) * 1000;

        return $elapsed < $this->stickyDurationMs;
    }
}
