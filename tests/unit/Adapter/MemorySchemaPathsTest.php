<?php

namespace Tests\Unit\Adapter;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;

final class MemorySchemaPathsTest extends TestCase
{
    private const string DATABASE = 'schema_paths';

    private const string COLLECTION = 'pairs';

    public function testRollbackUndoesCreateCollection(): void
    {
        $adapter = $this->adapter();

        $adapter->startTransaction();
        $this->assertTrue($adapter->createCollection('rolled'));
        $this->assertTrue($adapter->exists(self::DATABASE, 'rolled'));
        $adapter->rollbackTransaction();

        $this->assertFalse($adapter->exists(self::DATABASE, 'rolled'), 'A rolled-back collection must leave its database');
        $this->assertTrue($adapter->createCollection('rolled'));
    }

    public function testDeleteAttributeKeepsTheOrdersOfTheRemainingIndexAttributes(): void
    {
        [$adapter, $indexOf] = $this->inspectableAdapter();
        $this->createPairs($adapter);
        $adapter->createIndex(self::COLLECTION, Index::key(key: 'by_all', attributes: ['a', 'b', 'c'], orders: [OrderDirection::Asc, OrderDirection::Desc, OrderDirection::Asc]));

        $this->assertTrue($adapter->deleteAttribute(self::COLLECTION, 'a'));

        $index = $indexOf(self::COLLECTION, 'by_all');
        $this->assertSame(['b', 'c'], $index['attributes'] ?? null);
        $this->assertSame([OrderDirection::Desc->value, OrderDirection::Asc->value], \array_map(
            static fn (mixed $order): mixed => $order instanceof OrderDirection ? $order->value : $order,
            \is_array($index['orders'] ?? null) ? $index['orders'] : [],
        ));
    }

    public function testRollbackOfDeleteAttributeRestoresValuesAndIndexes(): void
    {
        [$adapter, $indexOf] = $this->inspectableAdapter();
        $this->createPairs($adapter);
        $adapter->createIndex(self::COLLECTION, Index::unique(key: 'unique_pair', attributes: ['a', 'b']));
        $adapter->createDocument($this->collection(), $this->pair('first', 'x', 'y'));

        $adapter->startTransaction();
        $adapter->deleteAttribute(self::COLLECTION, 'a');
        $this->assertNull($adapter->getDocument($this->collection(), 'first')->getAttribute('a'));
        $adapter->rollbackTransaction();

        $this->assertSame('x', $adapter->getDocument($this->collection(), 'first')->getAttribute('a'));
        $this->assertSame(['a', 'b'], $indexOf(self::COLLECTION, 'unique_pair')['attributes'] ?? null);

        $this->expectException(DuplicateException::class);
        $adapter->createDocument($this->collection(), $this->pair('second', 'x', 'y'));
    }

    public function testRollbackOfRenameAttributeRestoresTheOldName(): void
    {
        [$adapter, $indexOf] = $this->inspectableAdapter();
        $this->createPairs($adapter);
        $adapter->createIndex(self::COLLECTION, Index::key(key: 'by_a', attributes: ['a']));
        $adapter->createDocument($this->collection(), $this->pair('first', 'x', 'y'));

        $adapter->startTransaction();
        $this->assertTrue($adapter->renameAttribute(self::COLLECTION, 'a', 'renamed'));
        $this->assertSame(['renamed'], $indexOf(self::COLLECTION, 'by_a')['attributes'] ?? null);
        $adapter->rollbackTransaction();

        $stored = $adapter->getDocument($this->collection(), 'first');
        $this->assertSame('x', $stored->getAttribute('a'));
        $this->assertNull($stored->getAttribute('renamed'));
        $this->assertSame(['a'], $indexOf(self::COLLECTION, 'by_a')['attributes'] ?? null);
        $this->assertSame(['first'], $this->idsOf($adapter->find($this->collection(), [Query::equal('a', ['x'])])));
    }

    public function testUnvalidatedQueriesFollowSqlNullAndMethodRules(): void
    {
        $adapter = $this->adapter();
        $this->createPairs($adapter);
        $adapter->createDocument($this->collection(), $this->pair('first', 'x', 'y'));
        $adapter->createDocument($this->collection(), $this->pair('second', 'z', 'y'));

        $this->assertSame([], $adapter->find($this->collection(), [new Query(Method::NotEqual, 'a', [null, 'x'])]), 'A null candidate makes NOT IN unknown for every row');
        $this->assertSame([], $adapter->find($this->collection(), [new Query(Method::Regex, 'a', [5])]), 'A non-string pattern matches nothing');

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Query method not implemented in the Memory adapter: exists');
        $adapter->find($this->collection(), [Query::exists(['a'])]);
    }

    public function testUnorderedReadReturnsRowsInSequenceOrder(): void
    {
        $adapter = $this->adapter();
        $this->createPairs($adapter);
        foreach (['first', 'second', 'third'] as $id) {
            $adapter->createDocument($this->collection(), $this->pair($id, $id, 'y'));
        }
        $adapter->updateDocument($this->collection(), 'first', new Document(['$id' => 'renamed']), true);

        $this->assertSame(['renamed', 'second', 'third'], $this->idsOf($adapter->find($this->collection())));
        $this->assertSame(['second'], $this->idsOf($adapter->find($this->collection(), limit: 1, offset: 1)));
    }

    private function adapter(): Memory
    {
        return $this->prepare(new Memory());
    }

    /**
     * @return array{Memory, Closure(string, string): array<string, mixed>}
     */
    private function inspectableAdapter(): array
    {
        $adapter = new class () extends Memory {
            /**
             * @return array<string, mixed>
             */
            public function indexOf(string $collection, string $index): array
            {
                return $this->data[$this->key($collection)]['indexes'][$index] ?? [];
            }
        };

        return [$this->prepare($adapter), $adapter->indexOf(...)];
    }

    private function prepare(Memory $adapter): Memory
    {
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->setDatabase(self::DATABASE);
        $adapter->setNamespace('schema_paths_'.\uniqid());
        $adapter->create(self::DATABASE);

        return $adapter;
    }

    private function createPairs(Memory $adapter): void
    {
        $adapter->createCollection(self::COLLECTION);
        foreach (['a', 'b', 'c'] as $attribute) {
            $adapter->createAttribute(self::COLLECTION, Attribute::string(key: $attribute, size: 32));
        }
    }

    private function pair(string $id, string $a, string $b): Document
    {
        return new Document(['$id' => $id, '$permissions' => [], 'a' => $a, 'b' => $b]);
    }

    private function collection(): Document
    {
        return new Document(['$id' => self::COLLECTION]);
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function idsOf(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }
}
