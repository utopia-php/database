<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

final class DistinctCapabilityTest extends TestCase
{
    private const string COLLECTION = 'colours';

    public function testUnvalidatedDistinctIsRefusedWithoutAggregationSupport(): void
    {
        $database = $this->database(new Memory());
        $this->assertFalse($database->getAdapter()->supports(Capability::Aggregations));

        try {
            $database->skipValidation(fn (): array => $database->find(self::COLLECTION, [
                Query::select(['name']),
                Query::distinct(),
            ]));
            $this->fail('A distinct() read must be refused by an adapter that cannot deduplicate rows');
        } catch (QueryException $exception) {
            $this->assertSame('Distinct queries are not supported by this adapter', $exception->getMessage());
        }
    }

    public function testValidatedDistinctIsRefusedWithoutAggregationSupport(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(QueryException::class);
        $database->find(self::COLLECTION, [Query::select(['name']), Query::distinct()]);
    }

    public function testDistinctDeduplicatesWithAggregationSupport(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $this->assertTrue($database->getAdapter()->supports(Capability::Aggregations));

        $rows = $database->skipValidation(fn (): array => $database->find(self::COLLECTION, [
            Query::select(['name']),
            Query::distinct(),
        ]));

        $this->assertSame(['red'], \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->addHook(new Permissions());
        $database
            ->setDatabase('distinct_capability')
            ->setNamespace('distinct_capability_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 32, required: false)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        ));

        foreach (['first', 'second'] as $id) {
            $database->createDocument(self::COLLECTION, new Document([
                '$id' => $id,
                'name' => 'red',
            ]));
        }

        return $database;
    }
}
