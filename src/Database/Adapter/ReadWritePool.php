<?php

namespace Utopia\Database\Adapter;

use Swoole\Coroutine;
use Swoole\Coroutine\Context;
use Utopia\Database\Adapter;
use Utopia\Database\PermissionType;
use Utopia\Pools\Pool as UtopiaPool;

class ReadWritePool extends Pool
{
    final protected const array READ_METHODS = [
        'find',
        'getDocument',
        'count',
        'sum',
        'exists',
        'collectionExists',
        'list',
        'getSchemaAttributes',
        'getSchemaIndexes',
        'getSizeOfCollection',
        'getSizeOfCollectionOnDisk',
        'ping',
        'id',
    ];

    /**
     * Calls that neither read nor write data. They go wherever a read would and never open
     * the sticky window, so a call that writes, or must see the latest write, never belongs here.
     */
    final protected const array METADATA_METHODS = [
        'supports',
        'capabilities',
        'hasFeature',
        'limits',
        'isSchemaless',
        'setSchemaless',
        'builder',
        'schema',
        'getColumnType',
        'getSchemaIndexType',
        'encode',
        'decode',
        'castBefore',
        'castAfter',
        'castDatetime',
        'getAttributeWidth',
        'getCountOfAttributes',
        'getCountOfIndexes',
    ];

    /**
     * Metadata the write pool answers however reads are routed: the hostname namespaces
     * document and query cache keys, so it must not change with the pool a read goes to.
     */
    final protected const array WRITE_POOL_METADATA_METHODS = [
        'hostname',
    ];

    private const string REPLICA_READ = 'utopia.database.replicaRead.';

    /**
     * @var UtopiaPool<covariant Adapter>
     */
    private UtopiaPool $readPool;

    private bool $sticky = true;

    private int $stickyDurationMs = 5000;

    private ?float $lastWriteTimestamp = null;

    private ?string $writePoolHostname = null;

    private bool $replicaRead = false;

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

    #[\Override]
    public function delegate(string $method, array $arguments): mixed
    {
        return $this->borrowAndInvoke($method, $arguments);
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
    public function hostname(): string
    {
        return $this->writePoolHostname ??= parent::hostname();
    }

    /**
     * Whether the calling coroutine's latest read was served by a replica, which may lag the
     * primary: what it returned must not be cached for other readers.
     */
    public function servedByReplica(): bool
    {
        $context = $this->getCoroutineContext();
        if ($context === null) {
            return $this->replicaRead;
        }

        return ($context[$this->getReplicaReadKey()] ?? false) === true;
    }

    /**
     * @return UtopiaPool<covariant Adapter>
     */
    #[\Override]
    protected function getReadPool(): UtopiaPool
    {
        return $this->isSticky() ? parent::getReadPool() : $this->readPool;
    }

    /**
     * @param  array<mixed>  $arguments
     * @param  class-string|null  $feature
     */
    #[\Override]
    protected function borrowAndInvoke(string $method, array $arguments, ?string $feature = null): mixed
    {
        if ($this->isWrite($method, $arguments)) {
            $this->recordRead($method, false);
            try {
                return parent::borrowAndInvoke($method, $arguments, $feature);
            } finally {
                $this->stick();
            }
        }

        if ($this->pin() !== null || $this->isSticky() || \in_array($method, self::WRITE_POOL_METADATA_METHODS, true)) {
            $this->recordRead($method, false);

            return parent::borrowAndInvoke($method, $arguments, $feature);
        }

        $this->recordRead($method, true);

        return $this->readPool->use(function (Adapter $adapter) use ($method, $arguments, $feature) {
            try {
                $this->syncBorrowed($adapter);

                return $this->invokeDelegated($adapter, $method, $arguments, $feature);
            } finally {
                $this->releaseBorrowed($adapter);
            }
        });
    }

    /**
     * @param  array<mixed>  $arguments
     */
    private function isWrite(string $method, array $arguments): bool
    {
        if ($this->decidesWrite($method, $arguments)) {
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
     * @param  array<mixed>  $arguments
     */
    private function decidesWrite(string $method, array $arguments): bool
    {
        return match ($method) {
            'getDocument' => ($arguments[3] ?? $arguments['forUpdate'] ?? false) === true,
            'find' => ($arguments[8] ?? $arguments['forPermission'] ?? PermissionType::Read) !== PermissionType::Read,
            default => false,
        };
    }

    private function recordRead(string $method, bool $replica): void
    {
        if (! \in_array($method, self::READ_METHODS, true)) {
            return;
        }

        $context = $this->getCoroutineContext();
        if ($context === null) {
            $this->replicaRead = $replica;

            return;
        }

        $context[$this->getReplicaReadKey()] = $replica;
    }

    private function getCoroutineContext(): ?Context
    {
        if (! \extension_loaded('swoole')) {
            return null;
        }

        $context = Coroutine::getContext();

        return $context instanceof Context ? $context : null;
    }

    private function getReplicaReadKey(): string
    {
        return self::REPLICA_READ.\spl_object_id($this);
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
