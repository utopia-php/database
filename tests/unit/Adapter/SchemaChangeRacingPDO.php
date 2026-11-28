<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;

/**
 * Reports a CREATE TABLE for a table that already exists the way SQLite does when the losing create is re-prepared
 * while the winner's next schema change lands: SQLITE_SCHEMA (17) carrying the "already exists" message.
 */
final class SchemaChangeRacingPDO extends PDO
{
    private const int SQLITE_ERROR = 1;

    private const int SQLITE_SCHEMA = 17;

    public function __construct()
    {
        parent::__construct('sqlite::memory:');
    }

    /**
     * @param  array<int, mixed>  $options
     */
    #[\Override]
    public function prepare(string $query, array $options = []): PDOStatement|false
    {
        try {
            return parent::prepare($query, $options);
        } catch (PDOException $error) {
            if (($error->errorInfo[1] ?? null) !== self::SQLITE_ERROR || \stripos($error->getMessage(), 'already exists') === false) {
                throw $error;
            }

            throw self::schemaChanged($error);
        }
    }

    private static function schemaChanged(PDOException $error): PDOException
    {
        $message = \str_replace('General error: '.self::SQLITE_ERROR.' ', 'General error: '.self::SQLITE_SCHEMA.' ', $error->getMessage());

        $schemaChanged = new class ($message, $error) extends PDOException {
            public function __construct(string $message, PDOException $previous)
            {
                parent::__construct($message, previous: $previous);
                $this->code = $previous->getCode();
            }
        };
        $schemaChanged->errorInfo = [$error->errorInfo[0] ?? 'HY000', self::SQLITE_SCHEMA, $error->errorInfo[2] ?? $message];

        return $schemaChanged;
    }
}
