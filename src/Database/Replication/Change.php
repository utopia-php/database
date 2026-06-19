<?php

namespace Utopia\Database\Replication;

/**
 * A single row-change decoded from the binlog.
 *
 * For UPDATE events, {@see $rows} holds the after-image of each updated row.
 */
final class Change
{
    public const string INSERT = 'insert';
    public const string UPDATE = 'update';
    public const string DELETE = 'delete';

    /**
     * @param string $action One of INSERT|UPDATE|DELETE.
     * @param string $database Source schema name.
     * @param string $table Physical table name (e.g. "console15x_projects").
     * @param array<int, array<string, mixed>> $rows Affected rows as column => value maps.
     * @param string $gtid Executed-GTID-set after this event — a resumable checkpoint token.
     */
    public function __construct(
        public readonly string $action,
        public readonly string $database,
        public readonly string $table,
        public readonly array $rows,
        public readonly string $gtid,
    ) {
    }
}
