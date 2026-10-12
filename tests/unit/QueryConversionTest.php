<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
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
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Query\Schema\ColumnType;

final class QueryConversionTest extends TestCase
{
    private const string COLLECTION = 'events';

    /**
     * @return array<string, array{\Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUnparseableDatetimeQueryValueIsAQueryException(\Closure $adapter): void
    {
        $database = $this->eventsDatabase($adapter());

        try {
            $database->convertQuery($database->getCollection(self::COLLECTION), Query::equal('occurredAt', ['not-a-date']));
            $this->fail('convertQuery() must refuse an unparseable datetime');
        } catch (QueryException $error) {
            $this->assertNotNull($error->getPrevious(), 'the parse failure is kept as the previous exception');
        }

        $this->expectException(QueryException::class);
        $database->skipValidation(fn (): array => $database->find(self::COLLECTION, [Query::equal('occurredAt', ['not-a-date'])]));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAttributeWithoutAStringTypeIsRefused(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        $collection = new Document([
            Document::ID => self::COLLECTION,
            'attributes' => [new Document([Document::ID => 'untyped', 'type' => 5, 'array' => true])],
        ]);

        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('Attribute type must be a string, int given');
        $database->convertQuery($collection, Query::equal('untyped', ['x']));
    }

    /**
     * @return array<string, array{Query, string}>
     */
    public static function undeclaredValues(): array
    {
        return [
            'map value' => [Query::equal('coords', [['x' => 1]]), ColumnType::Object->value],
            'empty map value' => [Query::equal('coords', [[]]), ColumnType::Object->value],
            'list value' => [Query::equal('coords', [[1, 2]]), ''],
            'list after a map' => [Query::equal('coords', [['x' => 1], [1, 2]]), ''],
            'scalar value' => [Query::equal('coords', ['x']), ''],
            'no values' => [Query::isNull('coords'), ''],
        ];
    }

    #[DataProvider('undeclaredValues')]
    public function testOnlyMapValuesOfAnUndeclaredAttributeAreObjectQueries(Query $query, string $type): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function capabilities(): array
            {
                return \array_values(\array_filter(
                    parent::capabilities(),
                    static fn (Capability $capability): bool => $capability !== Capability::DefinedAttributes,
                ));
            }
        });

        $converted = $database->convertQuery(new Document([Document::ID => self::COLLECTION, 'attributes' => []]), $query);

        $this->assertSame($type, $converted->getAttributeType());
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testConvertQueryMatchesConvertQueries(\Closure $adapter): void
    {
        $database = $this->eventsDatabase($adapter());
        $collection = $database->getCollection(self::COLLECTION);

        foreach ([
            static fn (): Query => Query::equal('occurredAt', ['2026-09-30 10:00:00']),
            static fn (): Query => Query::containsAny('tags', ['a']),
            static fn (): Query => Query::equal(Document::CREATED_AT, ['2026-09-30 10:00:00']),
        ] as $build) {
            $single = $database->convertQuery($collection, $build());
            [$listed] = $database->convertQueries($collection, [$build()]);

            $this->assertNotSame('', $single->getAttributeType(), $single->getAttribute());
            $this->assertSame($listed->getAttributeType(), $single->getAttributeType(), $single->getAttribute());
            $this->assertSame($listed->onArray(), $single->onArray(), $single->getAttribute());
            $this->assertSame($listed->getValues(), $single->getValues(), $single->getAttribute());
        }

        $this->assertTrue($database->convertQuery($collection, Query::containsAny('tags', ['a']))->onArray());

        if ($database->getAdapter()->supports(Capability::Objects)) {
            $database->createAttribute(self::COLLECTION, Attribute::object(key: 'meta'));
            $withObject = $database->getCollection(self::COLLECTION);
            $this->assertSame(
                ColumnType::Object->value,
                $database->convertQuery($withObject, Query::equal('meta.level', ['x']))->getAttributeType(),
                'a path into an object attribute converts as an object query',
            );
        }
        $this->assertNotSame(
            ['2026-09-30 10:00:00'],
            $database->convertQuery($collection, Query::equal('occurredAt', ['2026-09-30 10:00:00']))->getValues(),
            'a datetime value is converted to the storage format',
        );
    }

    private function eventsDatabase(Adapter $adapter): Database
    {
        $database = $this->database($adapter);
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::datetime(key: 'occurredAt'),
                Attribute::string(key: 'tags', size: 16, array: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('conversion')->setNamespace('conversion_'.\uniqid());
        $database->create();

        return $database;
    }
}
