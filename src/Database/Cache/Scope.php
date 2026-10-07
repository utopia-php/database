<?php

namespace Utopia\Database\Cache;

/**
 * What a query cache entry belongs to, and the settings of the database it is read or written through: one query
 * cache shared by several databases keys and times out each call by the scope it is handed, never by one database.
 */
final readonly class Scope
{
    public const string NAME = 'default';

    public const int WRITER_TIMEOUT = 3600;

    /**
     * @param  string  $name  The database's cache name, which keeps apart the results of databases named differently
     * @param  int  $writerTimeout  Seconds after which a write that has not activated is treated as abandoned
     */
    public function __construct(
        public string $hostname = '',
        public string $database = '',
        public string $namespace = '',
        public int|string|null $tenant = null,
        public string $name = self::NAME,
        public int $writerTimeout = self::WRITER_TIMEOUT,
    ) {
    }

    public function withTenant(int|string|null $tenant): self
    {
        return new self(
            hostname: $this->hostname,
            database: $this->database,
            namespace: $this->namespace,
            tenant: $tenant,
            name: $this->name,
            writerTimeout: $this->writerTimeout,
        );
    }
}
