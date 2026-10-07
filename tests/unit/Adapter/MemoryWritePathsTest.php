<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;

final class MemoryWritePathsTest extends TestCase
{
    private const string COLLECTION = 'addresses';

    private const string UPDATED_AT = '2026-01-01 00:00:00.000';

    public function testSharedTablesUniqueIndexOverExistingRowsIsPerTenant(): void
    {
        $adapter = $this->sharedAdapter();
        foreach ([1, 2] as $tenant) {
            $adapter->setTenant($tenant);
            $this->storeAddress($adapter, 'home', 'x');
        }

        $adapter->setTenant(1);
        $this->assertTrue($adapter->createIndex(self::COLLECTION, Index::unique(key: 'unique_addr', attributes: ['addr'])), 'Two tenants holding the same value must not block a unique index');

        $this->assertDuplicate(fn () => $this->storeAddress($adapter, 'second', 'x'), 'A same-tenant duplicate must be rejected once the index exists');

        $adapter->setTenant(2);
        $this->assertSame('x', $adapter->getDocument($this->collection(), 'home')->getAttribute('addr'));
    }

    public function testSharedTablesUniqueIndexRejectsDuplicatesWithinOneTenant(): void
    {
        $adapter = $this->sharedAdapter();
        $adapter->setTenant(1);
        $this->storeAddress($adapter, 'first', 'x');
        $this->storeAddress($adapter, 'second', 'x');

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Cannot create unique index: existing rows already contain duplicate values');
        $adapter->createIndex(self::COLLECTION, Index::unique(key: 'unique_addr', attributes: ['addr']));
    }

    public function testRolledBackUniqueIndexLeavesNoHashTable(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @return array<string, mixed>
             */
            public function uniqueHashesOf(string $collection): array
            {
                return $this->uniqueIndexHashes[$this->key($collection)] ?? [];
            }
        };
        $adapter->setNamespace('unique_rollback_'.\uniqid());
        $this->createAddresses($adapter);
        $this->storeAddress($adapter, 'home', 'x');

        $adapter->startTransaction();
        $adapter->createIndex(self::COLLECTION, Index::unique(key: 'unique_addr', attributes: ['addr']));
        $this->assertArrayHasKey('unique_addr', $adapter->uniqueHashesOf(self::COLLECTION));
        $adapter->rollbackTransaction();

        $this->assertSame([], $adapter->uniqueHashesOf(self::COLLECTION));
    }

    public function testSharedTablesUniqueBindingsFollowTheirTenantOnUpdateAndDelete(): void
    {
        $adapter = $this->sharedAdapter();
        $adapter->setTenant(1);
        $adapter->createIndex(self::COLLECTION, Index::unique(key: 'unique_addr', attributes: ['addr']));
        foreach ([1, 2] as $tenant) {
            $adapter->setTenant($tenant);
            $this->storeAddress($adapter, 'home', 'x');
        }

        $adapter->setTenant(2);
        $adapter->updateDocument($this->collection(), 'home', new Document(['$id' => 'home', 'addr' => 'x', 'label' => 'kept']), true);
        $this->assertTrue($adapter->deleteDocument(self::COLLECTION, 'home'));

        $adapter->setTenant(1);
        $this->assertDuplicate(fn () => $this->storeAddress($adapter, 'second', 'x'), 'The first tenant\'s binding must survive the second tenant\'s update and delete');

        $adapter->setTenant(2);
        $this->storeAddress($adapter, 'again', 'x');
        $this->assertSame('x', $adapter->getDocument($this->collection(), 'again')->getAttribute('addr'));
    }

    public function testRollbackRestoresARenamedDocument(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'a', 'addr' => 'x', 'label' => 'original']));

        $rethrown = false;
        try {
            $database->withTransaction(function () use ($database): void {
                $database->updateDocument(self::COLLECTION, 'a', new Document(['$id' => 'b', 'label' => 'renamed']));
                throw new \RuntimeException('roll back');
            });
        } catch (\RuntimeException $exception) {
            $rethrown = $exception->getMessage() === 'roll back';
        }
        $this->assertTrue($rethrown, 'The transaction must rethrow');

        $this->assertSame('original', $database->getDocument(self::COLLECTION, 'a')->getAttribute('label'));
        $this->assertTrue($database->getDocument(self::COLLECTION, 'b')->isEmpty());
        $this->assertSame(['a'], \array_map(static fn (Document $document): string => $document->getId(), $database->find(self::COLLECTION)));
    }

    public function testRollbackUndoesAnIncrement(): void
    {
        $adapter = $this->adapter();
        $this->storeAddress($adapter, 'home', 'x', 1);
        $before = $adapter->getDocument($this->collection(), 'home');

        $adapter->startTransaction();
        $this->assertTrue($adapter->increaseDocumentAttribute(self::COLLECTION, 'home', 'visits', 5, self::UPDATED_AT));
        $this->assertSame(6, $adapter->getDocument($this->collection(), 'home')->getAttribute('visits'));
        $adapter->rollbackTransaction();

        $after = $adapter->getDocument($this->collection(), 'home');
        $this->assertSame(1, $after->getAttribute('visits'));
        $this->assertSame($before->getUpdatedAt(), $after->getUpdatedAt());
    }

    public function testRollbackOfAnIncrementRemovesTheValueAndTimestampItAdded(): void
    {
        $adapter = $this->adapter();
        $adapter->createDocument($this->collection(), new Document(['$id' => 'home', '$permissions' => [], 'addr' => 'x']));
        $before = $adapter->getDocument($this->collection(), 'home');
        $this->assertNull($before->getAttribute('visits'));
        $this->assertNull($before->getUpdatedAt());

        $adapter->startTransaction();
        $this->assertTrue($adapter->increaseDocumentAttribute(self::COLLECTION, 'home', 'visits', 5, self::UPDATED_AT));
        $this->assertSame(5, $adapter->getDocument($this->collection(), 'home')->getAttribute('visits'));
        $adapter->rollbackTransaction();

        $after = $adapter->getDocument($this->collection(), 'home');
        $this->assertNull($after->getAttribute('visits'));
        $this->assertNull($after->getUpdatedAt());
    }

    public function testADivisionOrModuloByZeroThatSkippedValidationKeepsTheValue(): void
    {
        $adapter = $this->adapter();
        $this->storeAddress($adapter, 'home', 'x', 10);

        foreach (['divide', 'modulo'] as $method) {
            $operator = Operator::parse('{"method":"'.$method.'","attribute":"visits","values":[0]}');
            $adapter->updateDocument($this->collection(), 'home', new Document(['$id' => 'home', 'visits' => $operator]), true);

            $this->assertSame(10, $adapter->getDocument($this->collection(), 'home')->getAttribute('visits'), $method);
        }
    }

    public function testACursorWithoutAnOrderPagesBySequence(): void
    {
        $authorization = new Authorization();
        $authorization->disable();
        $adapter = $this->adapter();
        $adapter->setAuthorization($authorization);
        foreach (['first', 'second', 'third'] as $id) {
            $this->storeAddress($adapter, $id, 'x');
        }
        $cursor = ['$sequence' => $adapter->getDocument($this->collection(), 'second')->getSequence()];
        $ids = static function (array $documents): array {
            /** @var array<Document> $documents */
            return \array_map(static fn (Document $document): string => $document->getId(), $documents);
        };

        $this->assertSame(['third'], $ids($adapter->find($this->collection(), cursor: $cursor)));
        $this->assertSame(['first'], $ids($adapter->find($this->collection(), cursor: $cursor, cursorDirection: CursorDirection::Before)));
    }

    public function testIncrementIsANoOpWhenTheStoredValueAlreadyViolatesTheBound(): void
    {
        $adapter = $this->adapter();
        $this->storeAddress($adapter, 'whole', 'x', 10);
        $this->storeAddress($adapter, 'fraction', 'y', 10.5);

        foreach (['whole' => 10, 'fraction' => 10.5] as $id => $stored) {
            $this->assertTrue($adapter->increaseDocumentAttribute(self::COLLECTION, $id, 'visits', 1, self::UPDATED_AT, max: 5));
            $this->assertTrue($adapter->increaseDocumentAttribute(self::COLLECTION, $id, 'visits', -1, self::UPDATED_AT, min: 20));
            $this->assertSame($stored, $adapter->getDocument($this->collection(), $id)->getAttribute('visits'), $id);
        }
    }

    public function testBatchMixingDocumentsWithAndWithoutASequenceIsRejected(): void
    {
        foreach ([['10', null], [null, '10']] as [$first, $second]) {
            $adapter = $this->adapter();
            $documents = [
                $this->address('first', 'x', sequence: $first),
                $this->address('second', 'y', sequence: $second),
            ];

            try {
                $adapter->createDocuments($this->collection(), $documents);
                $this->fail('A batch mixing set and unset sequences must be rejected');
            } catch (DatabaseException $exception) {
                $this->assertSame('All documents must have an sequence if one is set', $exception->getMessage());
            }

            $this->assertSame([], $adapter->find($this->collection()));
        }
    }

    public function testNullChecksAndUnsupportedMethodsOnAWholeObjectAttribute(): void
    {
        $database = $this->database();
        $database->createAttribute(self::COLLECTION, Attribute::object(key: 'meta'));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'filled', 'addr' => 'x', 'meta' => ['colour' => 'red']]));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'empty', 'addr' => 'y', 'meta' => null]));

        $idsOf = static function (array $documents): array {
            /** @var array<Document> $documents */
            return \array_map(static fn (Document $document): string => $document->getId(), $documents);
        };

        $this->assertSame(['empty'], $idsOf($database->find(self::COLLECTION, [Query::isNull('meta')])));
        $this->assertSame(['filled'], $idsOf($database->find(self::COLLECTION, [Query::isNotNull('meta')])));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Query method lessThan not supported for object attributes');
        $database->skipValidation(fn (): array => $database->find(self::COLLECTION, [Query::lessThan('meta', 'x')]));
    }

    private function assertDuplicate(\Closure $write, string $message): void
    {
        try {
            $write();
        } catch (DuplicateException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());

            return;
        }

        $this->fail($message);
    }

    private function adapter(): Memory
    {
        $adapter = new Memory();
        $adapter->setNamespace('write_paths_'.\uniqid());
        $this->createAddresses($adapter);

        return $adapter;
    }

    private function sharedAdapter(): Memory
    {
        $adapter = new Memory();
        $adapter->setNamespace('write_paths_shared_'.\uniqid());
        $adapter->setSharedTables(true);
        $adapter->setTenant(1);
        $this->createAddresses($adapter);

        return $adapter;
    }

    private function createAddresses(Memory $adapter): void
    {
        $adapter->createCollection(self::COLLECTION);
        $adapter->createAttribute(self::COLLECTION, Attribute::string(key: 'addr', size: 128, required: true));
        $adapter->createAttribute(self::COLLECTION, Attribute::string(key: 'label', size: 32));
        $adapter->createAttribute(self::COLLECTION, Attribute::double(key: 'visits'));
    }

    private function storeAddress(Memory $adapter, string $id, string $addr, int|float|null $visits = null): void
    {
        $adapter->createDocument($this->collection(), $this->address($id, $addr, $visits));
    }

    private function address(string $id, string $addr, int|float|null $visits = null, ?string $sequence = null): Document
    {
        $document = new Document([
            '$id' => $id,
            '$permissions' => [],
            '$updatedAt' => '2025-01-01 00:00:00.000',
            'addr' => $addr,
            'visits' => $visits,
        ]);
        if ($sequence !== null) {
            $document->setAttribute('$sequence', $sequence);
        }

        return $document;
    }

    private function collection(): Document
    {
        return new Document(['$id' => self::COLLECTION]);
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('write_paths')
            ->setNamespace('write_paths_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'addr', size: 128, required: true),
                Attribute::string(key: 'label', size: 32),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));

        return $database;
    }
}
