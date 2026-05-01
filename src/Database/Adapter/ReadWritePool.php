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
        'getLimitForAttributes',
        'getLimitForIndexes',
        'getMaxIndexLength',
        'getMaxVarcharLength',
        'getMaxUIDLength',
        'getIdAttributeType',
        'getKeywords',
        'getInternalIndexesKeys',
        'supports',
        'capabilities',
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
        if ($this->pinnedAdapter !== null) {
            return $this->pinnedAdapter->{$method}(...$args);
        }

        if ($this->isReadOperation($method) && ! $this->isSticky()) {
            return $this->readPool->use(function (Adapter $adapter) use ($method, $args) {
                $this->syncConfig($adapter);

                return $adapter->{$method}(...$args);
            });
        }

        if (! $this->isReadOperation($method)) {
            $this->lastWriteTimestamp = \microtime(true);
        }

        return parent::delegate($method, $args);
    }

    private function isReadOperation(string $method): bool
    {
        return \in_array($method, self::READ_METHODS, true);
    }

    private function isSticky(): bool
    {
        if (! $this->sticky || $this->lastWriteTimestamp === null) {
            return false;
        }

        $elapsed = (\microtime(true) - $this->lastWriteTimestamp) * 1000;

        return $elapsed < $this->stickyDurationMs;
    }

    private function syncConfig(Adapter $adapter): void
    {
        $adapter->setDatabase($this->getDatabase());
        $adapter->setNamespace($this->getNamespace());
        $adapter->setSharedTables($this->getSharedTables());
        $adapter->setTenant($this->getTenant());
        $adapter->setTenantPerDocument($this->getTenantPerDocument());
        $adapter->setAuthorization($this->authorization);

        if ($this->getTimeout() > 0) {
            $adapter->setTimeout($this->getTimeout());
        }

        $adapter->resetDebug();
        foreach ($this->getDebug() as $key => $value) {
            $adapter->setDebug($key, $value);
        }

        $adapter->resetMetadata();
        foreach ($this->getMetadata() as $key => $value) {
            $adapter->setMetadata($key, $value);
        }

        $adapter->setProfiler($this->profiler);
        $adapter->resetTransforms();
        foreach ($this->queryTransforms as $tName => $tTransform) {
            $adapter->addTransform($tName, $tTransform);
        }
    }
}
