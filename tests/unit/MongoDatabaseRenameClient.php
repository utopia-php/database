<?php

namespace Tests\Unit;

use stdClass;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;

/**
 * A server holding databases of collections, for the commands a rename and an existence check send. A command
 * without a database runs on the client's own, 'default'; a database without collections is not listed.
 */
final class MongoDatabaseRenameClient extends Client
{
    public ?string $failOn = null;

    public int $cursor = 0;

    /**
     * @param  array<string, list<string>>  $databases
     */
    public function __construct(public array $databases)
    {
    }

    #[\Override]
    public function connect(): self
    {
        return $this;
    }

    #[\Override]
    public function close(): void
    {
    }

    #[\Override]
    public function listDatabaseNames(): stdClass
    {
        $listed = new stdClass();
        $listed->databases = \array_map(static function (string $name): stdClass {
            $database = new stdClass();
            $database->name = $name;

            return $database;
        }, \array_keys($this->databases));

        return $listed;
    }

    /**
     * @param  array<string, mixed>  $options
     */
    #[\Override]
    public function createCollection(string $name, array $options = []): bool
    {
        $this->databases['default'][] = $name;

        return true;
    }

    /**
     * @param  array<string, mixed>  $options
     */
    #[\Override]
    public function dropDatabase(array $options = [], ?string $db = null): bool
    {
        unset($this->databases[$db ?? 'default']);

        return true;
    }

    /**
     * @param  array<string, mixed>  $command
     */
    #[\Override]
    public function query(array $command, ?string $db = null): stdClass
    {
        $db ??= 'default';

        if (isset($command['renameCollection'])) {
            /** @var string $from */
            $from = $command['renameCollection'];
            /** @var string $to */
            $to = $command['to'];
            if ($from === $this->failOn) {
                throw new MongoException('rename refused');
            }
            [$fromDatabase, $collection] = \explode('.', $from, 2);
            [$toDatabase] = \explode('.', $to, 2);
            $this->databases[$fromDatabase] = \array_values(\array_diff($this->databases[$fromDatabase], [$collection]));
            if ($this->databases[$fromDatabase] === []) {
                unset($this->databases[$fromDatabase]);
            }
            $this->databases[$toDatabase][] = $collection;
            \sort($this->databases[$toDatabase]);

            return new stdClass();
        }

        $result = new stdClass();
        if (isset($command['listCollections'])) {
            /** @var array{name?: string} $filter */
            $filter = $command['filter'] ?? [];
            $names = \array_values(\array_filter(
                $this->databases[$db] ?? [],
                static fn (string $name): bool => ! isset($filter['name']) || $filter['name'] === $name,
            ));
            $cursor = new stdClass();
            $cursor->id = $this->cursor;
            $cursor->firstBatch = \array_map(static function (string $name): stdClass {
                $collection = new stdClass();
                $collection->name = $name;

                return $collection;
            }, $names);
            $result->cursor = $cursor;
        }

        return $result;
    }
}
