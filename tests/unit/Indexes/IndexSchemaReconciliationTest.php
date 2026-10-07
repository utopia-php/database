<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;

final class IndexSchemaReconciliationTest extends TestCase
{
    private const string COLLECTION = 'catalog';

    public function testAnAdapterThatDoesNotRenameTheIndexFailsTheRename(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
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

    public function testAnAdapterThatDoesNotCreateTheIndexFailsTheCreate(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
            {
                return $index->key === 'byName' ? false : parent::createIndex($collection, $index, $indexAttributeTypes, $collation);
            }
        });

        $this->assertRefused('Failed to create index', fn (): Index => $database->createIndex(self::COLLECTION, $this->byName()));
        $this->assertSame(['existing'], $this->indexKeys($database));
    }

    public function testAnIndexOnlyInTheSchemaIsAdopted(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
            {
                if ($index->key === 'byName') {
                    throw new DuplicateException('Index already exists in the schema');
                }

                return parent::createIndex($collection, $index, $indexAttributeTypes, $collation);
            }
        });

        $this->assertSame('byName', $database->createIndex(self::COLLECTION, $this->byName())->key);
        $this->assertSame(['existing', 'byName'], $this->indexKeys($database));
    }

    public function testRenamingAnUnknownIndexIsNotFound(): void
    {
        $database = $this->database(new Memory());

        try {
            $database->renameIndex(self::COLLECTION, 'missing', 'renamed');
            $this->fail('an unknown index cannot be renamed');
        } catch (NotFoundException $error) {
            $this->assertSame('Index not found', $error->getMessage());
        }

        $this->assertSame(['existing'], $this->indexKeys($database));
    }

    public function testRenamingAnIndexTheSchemaNoLongerHasFails(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter);
        $adapter->deleteIndex(self::COLLECTION, 'existing');

        try {
            $database->renameIndex(self::COLLECTION, 'existing', 'renamed');
            $this->fail('a rename of an index the schema does not have must fail');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to rename index 'existing' to 'renamed': Failed to rename index", $error->getMessage());
        }

        $this->assertSame(['existing'], $this->indexKeys($database));
        $this->assertFalse($adapter->renameIndex(self::COLLECTION, 'existing', 'renamed'));
    }

    public function testRenamingAnIndexTheSchemaAlreadyRenamedCompletes(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter);
        $this->assertTrue($adapter->renameIndex(self::COLLECTION, 'existing', 'renamed'));

        $database->renameIndex(self::COLLECTION, 'existing', 'renamed');
        $this->assertSame(['renamed'], $this->indexKeys($database));
        $this->assertTrue($adapter->renameIndex(self::COLLECTION, 'existing', 'renamed'), 'the index already carries the new name');
    }

    public function testARenameTheSchemaAlreadyAppliedIsCompleted(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @var list<string>
             */
            public array $renames = [];

            #[\Override]
            public function renameIndex(string $collection, string $old, string $new): bool
            {
                $this->renames[] = "{$old}->{$new}";
                if (\count($this->renames) === 1) {
                    throw new NotFoundException('Index not found in the schema');
                }

                return parent::renameIndex($collection, $old, $new);
            }
        };
        $database = $this->database($adapter);

        $database->renameIndex(self::COLLECTION, 'existing', 'renamed');
        $this->assertSame(['existing->renamed', 'renamed->existing', 'existing->renamed'], $adapter->renames);
        $this->assertSame(['renamed'], $this->indexKeys($database));
    }

    public function testARenameThatFailsBothWaysIsReportedWithItsCause(): void
    {
        $cause = new RuntimeException('the engine refused the rename');
        $database = $this->database(new class ($cause) extends Memory {
            public function __construct(private readonly RuntimeException $cause)
            {
                parent::__construct();
            }

            #[\Override]
            public function renameIndex(string $collection, string $old, string $new): bool
            {
                throw $this->cause;
            }
        });

        try {
            $database->renameIndex(self::COLLECTION, 'existing', 'renamed');
            $this->fail('a rename that fails both ways must be reported');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to rename index 'existing' to 'renamed': the engine refused the rename", $error->getMessage());
            $this->assertSame($cause, $error->getPrevious());
        }

        $this->assertSame(['existing'], $this->indexKeys($database));
    }

    public function testARenameTheAdapterDoesNotRedoAfterReversingItFails(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @var list<string>
             */
            public array $renames = [];

            #[\Override]
            public function renameIndex(string $collection, string $old, string $new): bool
            {
                $this->renames[] = "{$old}->{$new}";

                return \count($this->renames) === 2;
            }
        };
        $database = $this->database($adapter);

        try {
            $database->renameIndex(self::COLLECTION, 'existing', 'renamed');
            $this->fail('a rename the adapter does not redo must fail');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to rename index 'existing' to 'renamed': Failed to rename index", $error->getMessage());
        }

        $this->assertSame(['existing->renamed', 'renamed->existing', 'existing->renamed'], $adapter->renames);
        $this->assertSame(['existing'], $this->indexKeys($database));
    }

    public function testDeletingAnIndexTheSchemaNoLongerHasSucceeds(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function deleteIndex(string $collection, string $id): bool
            {
                throw new NotFoundException('Index not found in the schema');
            }
        });

        $database->deleteIndex(self::COLLECTION, 'existing');
        $this->assertSame([], $this->indexKeys($database));
    }

    public function testAnAdapterThatDoesNotDeleteTheIndexFailsTheDelete(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function deleteIndex(string $collection, string $id): bool
            {
                return false;
            }
        });

        $this->assertRefused('Failed to delete index', fn () => $database->deleteIndex(self::COLLECTION, 'existing'));
        $this->assertSame(['existing'], $this->indexKeys($database));
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database): array
    {
        return \array_map(static fn (Index $index): string => $index->key, $database->getCollection(self::COLLECTION)->indexes());
    }

    private function database(Memory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('indexes')->setNamespace('reconcile_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 32), Attribute::string(key: 'sku', size: 32)],
            indexes: [Index::key(key: 'existing', attributes: ['sku'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }

    private function byName(): Index
    {
        return Index::key(key: 'byName', attributes: ['name']);
    }

    /**
     * @param  callable(): mixed  $operation
     */
    private function assertRefused(string $message, callable $operation): void
    {
        try {
            $operation();
            $this->fail('the operation must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame($message, $error->getMessage());
        }
    }
}
