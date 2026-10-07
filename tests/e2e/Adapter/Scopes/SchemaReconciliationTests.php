<?php

namespace Tests\E2E\Adapter\Scopes;

use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Schema\Index as SchemaIndex;
use Utopia\Database\Storage;
use Utopia\Query\Schema\IndexType;

trait SchemaReconciliationTests
{
    public function testEveryAttributeReadsBackAsTheColumnTypeTheAdapterCreates(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->supports(Capability::SchemaIntrospection)) {
            $this->assertSame([], $database->getSchemaAttributes('reconcileTypes'));
            $this->assertSame([], $database->getSchemaIndexes('reconcileTypes'));

            return;
        }

        $attributes = [
            Attribute::string(key: 'short', size: 64),
            Attribute::string(key: 'long', size: 20000),
            Attribute::string(key: 'list', size: 64, array: true),
            Attribute::integer(key: 'count'),
            Attribute::bigInteger(key: 'total'),
            Attribute::double(key: 'ratio'),
            Attribute::boolean(key: 'active'),
            Attribute::datetime(key: 'born'),
        ];
        if ($adapter->supports(Capability::Objects)) {
            $attributes[] = Attribute::object(key: 'meta');
        }
        if ($adapter->supports(Capability::Vectors)) {
            $attributes[] = Attribute::vector(key: 'embedding', dimensions: 3);
        }

        $collection = 'reconcileTypes';
        $database->createCollection(Collection::create(id: $collection, attributes: $attributes, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));

        try {
            $columns = $this->getSchemaColumnTypes($database, $collection);

            foreach ($attributes as $attribute) {
                $this->assertSame($adapter->getColumnType($attribute), $columns[$attribute->key] ?? null, $attribute->key);
            }
            $this->assertArrayHasKey(Storage::UID, $columns, 'An internal column is read back as a column');
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testAnOrphanColumnOfTheRequestedTypeIsReused(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->supports(Capability::SchemaIntrospection)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'reconcileReusedColumn';
        $this->createReconciledCollection($database, $collection);

        try {
            $attribute = Attribute::string(key: 'nick', size: 64);
            $adapter->createAttribute($collection, $attribute);

            $this->assertSame('nick', $database->createAttribute($collection, $attribute)->key);
            $this->assertSame(['nick'], $this->getReconciledAttributeKeys($database, $collection));
            $this->assertSame($adapter->getColumnType($attribute), $this->getSchemaColumnTypes($database, $collection)['nick'] ?? null);

            $database->createDocument($collection, new Document([Document::ID => 'one', 'nick' => 'kept']));
            $this->assertSame('kept', $database->getDocument($collection, 'one')->getAttribute('nick'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testAnOrphanColumnOfAnotherTypeIsReplacedOrRefusedUnderSharedTables(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->supports(Capability::SchemaIntrospection)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'reconcileReplacedColumn';
        $this->createReconciledCollection($database, $collection);

        try {
            $orphan = Attribute::integer(key: 'nick');
            $requested = Attribute::string(key: 'nick', size: 64);
            $adapter->createAttribute($collection, $orphan);

            if ($database->hasSharedTables()) {
                try {
                    $database->createAttribute($collection, $requested);
                    $this->fail('A column another tenant may use must not be replaced under shared tables');
                } catch (DuplicateException $error) {
                    $this->assertSame('Attribute exists in the shared table with another type', $error->getMessage());
                }

                $this->assertSame([], $this->getReconciledAttributeKeys($database, $collection));
                $this->assertSame($adapter->getColumnType($orphan), $this->getSchemaColumnTypes($database, $collection)['nick'] ?? null);

                return;
            }

            $this->assertSame('nick', $database->createAttribute($collection, $requested)->key);
            $this->assertSame(['nick'], $this->getReconciledAttributeKeys($database, $collection));
            $this->assertSame($adapter->getColumnType($requested), $this->getSchemaColumnTypes($database, $collection)['nick'] ?? null);
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testAnOrphanIndexOfTheRequestedDefinitionIsReused(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if (! $adapter->supports(Capability::SchemaIntrospection)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'reconcileReusedIndex';
        $this->createReconciledCollection($database, $collection, [Attribute::string(key: 'name', size: 64)]);

        try {
            $index = Index::key(key: 'lookup', attributes: ['name']);
            $adapter->createIndex($collection, $index);

            if (! $this->hasSchemaIndex($database, $collection, 'lookup')) {
                $this->markTestSkipped('getSchemaIndexes() does not report this index under its key on this adapter');
            }

            $this->assertSame('lookup', $database->createIndex($collection, $index)->key);
            $this->assertSame(['lookup'], \array_map(
                static fn (Index $stored): string => $stored->key,
                $database->getCollection($collection)->indexes(),
            ));

            $lookups = \array_values(\array_filter(
                $database->getSchemaIndexes($collection),
                static fn (SchemaIndex $schemaIndex): bool => $schemaIndex->name === 'lookup',
            ));
            $this->assertCount(1, $lookups);
            $this->assertSame(IndexType::Key, $lookups[0]->type);
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function createReconciledCollection(Database $database, string $collection, array $attributes = []): void
    {
        try {
            $database->deleteCollection($collection);
        } catch (Throwable) {
        }

        $database->createCollection(Collection::create(id: $collection, attributes: $attributes, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));
    }

    /**
     * @return array<string, string> The type of each column by name
     */
    private function getSchemaColumnTypes(Database $database, string $collection): array
    {
        $types = [];
        foreach ($database->getSchemaAttributes($collection) as $column) {
            $types[$column->name] = $column->type;
        }

        return $types;
    }

    /**
     * @return list<string>
     */
    private function getReconciledAttributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection($collection)->attributes(),
        );
    }

    private function hasSchemaIndex(Database $database, string $collection, string $key): bool
    {
        foreach ($database->getSchemaIndexes($collection) as $schemaIndex) {
            if ($schemaIndex->name === $key) {
                return true;
            }
        }

        return false;
    }
}
