<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\Schema\IndexType;

final class IndexSchemaReconciliationTest extends TestCase
{
    private const string COLLECTION = 'catalog';

    public function testAnAdapterThatDoesNotRenameTheIndexFailsTheRename(): void
    {
        $database = $this->database(new class () extends Memory {
            public function renameIndex(string $collection, string $old, string $new): bool
            {
                return false;
            }
        });

        try {
            $database->renameIndex(self::COLLECTION, 'existing', 'renamed');
            $this->fail('an adapter that renames nothing must fail the rename');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to rename index 'existing' to 'renamed': Failed to rename index", $error->getMessage());
        }

        $this->assertSame(['existing'], $this->indexKeys($database));
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database): array
    {
        /** @var array<Index> $indexes */
        $indexes = $database->getCollection(self::COLLECTION)->getAttribute('indexes', []);

        return \array_values(\array_map(static fn (Index $index): string => $index->key, $indexes));
    }

    private function database(Memory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('indexes')->setNamespace('reconcile_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32), Attribute::string(key: 'sku', size: 32)],
            indexes: [new Index(key: 'existing', type: IndexType::Key, attributes: ['sku'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
