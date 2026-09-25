<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\PermissionType;
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
        'getSizeOfCollection',
        'getSizeOfCollectionOnDisk',
        'ping',
        'getConnectionId',
    ];

    /**
     * Calls that neither read nor write data. They go wherever a read would and never open
     * the sticky window, so a call that writes, or must see the latest write, never belongs here.
     */
    private const METADATA_METHODS = [
        'supports',
        'capabilities',
        'hasFeature',
        'setSupportForAttributes',
        'getSupportNonUtfCharacters',
        'getBuilder',
        'getSchema',
        'getColumnType',
        'decodePoint',
        'decodeLinestring',
        'decodePolygon',
        'castingBefore',
        'castingAfter',
        'setUTCDatetime',
        'quote',
        'getAttributeProjection',
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
    ];

    /**
     * Metadata the write pool answers however reads are routed: the hostname namespaces
     * document and query cache keys, so it must not change with the pool a read goes to.
     */
    private const WRITE_POOL_METADATA_METHODS = [
        'getHostname',
    ];

    /**
     * @var UtopiaPool<covariant Adapter>
     */
    private UtopiaPool $readPool;

    private bool $sticky = true;

    private int $stickyDurationMs = 5000;

    private ?float $lastWriteTimestamp = null;

    private ?string $writePoolHostname = null;

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

    #[\Override]
    public function getHostname(): string
    {
        return $this->writePoolHostname ??= parent::getHostname();
    }

    /**
     * @param  array<mixed>  $args
     * @param  class-string|null  $feature
     */
    #[\Override]
    protected function borrowAndInvoke(string $method, array $args, ?string $feature = null): mixed
    {
        if ($this->isWrite($method, $args)) {
            try {
                return parent::borrowAndInvoke($method, $args, $feature);
            } finally {
                $this->stick();
            }
        }

        if ($this->pin() !== null || $this->isSticky() || \in_array($method, self::WRITE_POOL_METADATA_METHODS, true)) {
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
    private function isWrite(string $method, array $args): bool
    {
        if ($this->decidesWrite($method, $args)) {
            return true;
        }

        return ! \in_array($method, self::READ_METHODS, true)
            && ! \in_array($method, self::METADATA_METHODS, true)
            && ! \in_array($method, self::WRITE_POOL_METADATA_METHODS, true);
    }

    /**
     * A read whose result decides a write must see the primary: a lagging replica would select
     * rows the primary has already changed, or miss rows it has already written.
     *
     * @param  array<mixed>  $args
     */
    private function decidesWrite(string $method, array $args): bool
    {
        return match ($method) {
            'getDocument' => ($args[3] ?? $args['forUpdate'] ?? false) === true,
            'find' => ($args[8] ?? $args['forPermission'] ?? PermissionType::Read) !== PermissionType::Read,
            default => false,
        };
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
