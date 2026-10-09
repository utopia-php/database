<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Filter;
use Utopia\Database\Format;
use Utopia\Database\Id;
use Utopia\Database\Index;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Method;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Range;

trait AttributeTests
{
    private static string $attributesCollection = '';

    private static string $flowersCollection = '';

    private static string $colorsCollection = '';

    protected function getAttributesCollection(): string
    {
        if (self::$attributesCollection === '') {
            self::$attributesCollection = 'attributes_' . uniqid();
        }
        return self::$attributesCollection;
    }

    protected function getFlowersCollection(): string
    {
        if (self::$flowersCollection === '') {
            self::$flowersCollection = 'flowers_' . uniqid();
        }
        return self::$flowersCollection;
    }

    protected function getColorsCollection(): string
    {
        if (self::$colorsCollection === '') {
            self::$colorsCollection = 'colors_' . uniqid();
        }
        return self::$colorsCollection;
    }

    private function createRandomString(int $length = 10): string
    {
        return \substr(\bin2hex(\random_bytes(\max(1, \intval(($length + 1) / 2)))), 0, $length);
    }

    /**
     * @param  array<string, mixed>  $attribute
     */
    private function priceRangeFormat(array $attribute): Range
    {
        $formatOptions = $attribute['formatOptions'] ?? [];
        if (! is_array($formatOptions)) {
            $formatOptions = [];
        }
        $min = $formatOptions['min'] ?? 0;
        $max = $formatOptions['max'] ?? 0;
        if (! is_numeric($min)) {
            $min = 0;
        }
        if (! is_numeric($max)) {
            $max = 0;
        }

        return new Range((float) $min, (float) $max);
    }

    /**
     * @return list<array{0: ColumnType, 1: bool|float|int|string}>
     */
    public static function invalidDefaultValues(): array
    {
        return [
            [ColumnType::String, 1],
            [ColumnType::String, 1.5],
            [ColumnType::String, false],
            [ColumnType::Integer, 'one'],
            [ColumnType::Integer, 1.5],
            [ColumnType::Integer, true],
            [ColumnType::Double, 1],
            [ColumnType::Double, 'one'],
            [ColumnType::Double, false],
            [ColumnType::Boolean, 0],
            [ColumnType::Boolean, 'false'],
            [ColumnType::Boolean, 0.5],
            [ColumnType::Varchar, 1],
            [ColumnType::Varchar, 1.5],
            [ColumnType::Varchar, false],
            [ColumnType::Text, 1],
            [ColumnType::Text, 1.5],
            [ColumnType::Text, true],
            [ColumnType::MediumText, 1],
            [ColumnType::MediumText, 1.5],
            [ColumnType::MediumText, false],
            [ColumnType::LongText, 1],
            [ColumnType::LongText, 1.5],
            [ColumnType::LongText, true],
        ];
    }

    public function testCreateDeleteAttribute(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: $this->getAttributesCollection()));

        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string1', size: 128, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string2', size: 16382 + 1, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string3', size: 65535 + 1, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string4', size: 16777215 + 1, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::integer(key: 'integer', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::bigInteger(key: 'bigint', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::double(key: 'float', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'boolean', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::id(key: 'id', required: true));

        // New string types
        $database->createAttribute($this->getAttributesCollection(), Attribute::varchar(key: 'varchar1', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::varchar(key: 'varchar2', size: 128, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::text(key: 'text1', size: 65535, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::mediumText(key: 'mediumtext1', size: 16777215, required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::longText(key: 'longtext1', size: 4294967295, required: true));

        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'id_index', attributes: ['id']));
        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'string1_index', attributes: ['string1']));
        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'string2_index', attributes: ['string2'], lengths: [255]));
        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'multi_index', attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128]));
        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'varchar1_index', attributes: ['varchar1']));
        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'varchar2_index', attributes: ['varchar2']));
        $database->createIndex($this->getAttributesCollection(), Index::key(key: 'text1_index', attributes: ['text1'], lengths: [255]));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(14, $collection->attributes());
        $this->assertCount(7, $collection->indexes());

        // Array
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string_list', size: 128, required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::integer(key: 'integer_list', required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::double(key: 'float_list', required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'boolean_list', required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::varchar(key: 'varchar_list', size: 128, required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::text(key: 'text_list', size: 65535, required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::mediumText(key: 'mediumtext_list', size: 16777215, required: true, array: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::longText(key: 'longtext_list', size: 4294967295, required: true, array: true));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(22, $collection->attributes());

        // Default values
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string_default', size: 256, default: 'test'));
        $database->createAttribute($this->getAttributesCollection(), Attribute::integer(key: 'integer_default', default: 1));
        $database->createAttribute($this->getAttributesCollection(), Attribute::double(key: 'float_default', default: 1.5));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'boolean_default', default: false));
        $database->createAttribute($this->getAttributesCollection(), Attribute::datetime(key: 'datetime_default', default: '2000-06-12T14:12:55.000+00:00'));
        $database->createAttribute($this->getAttributesCollection(), Attribute::varchar(key: 'varchar_default', default: 'varchar default'));
        $database->createAttribute($this->getAttributesCollection(), Attribute::text(key: 'text_default', size: 65535, default: 'text default'));
        $database->createAttribute($this->getAttributesCollection(), Attribute::mediumText(key: 'mediumtext_default', size: 16777215, default: 'mediumtext default'));
        $database->createAttribute($this->getAttributesCollection(), Attribute::longText(key: 'longtext_default', size: 4294967295, default: 'longtext default'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(31, $collection->attributes());

        // Delete
        $database->deleteAttribute($this->getAttributesCollection(), 'string1');
        $database->deleteAttribute($this->getAttributesCollection(), 'string2');
        $database->deleteAttribute($this->getAttributesCollection(), 'string3');
        $database->deleteAttribute($this->getAttributesCollection(), 'string4');
        $database->deleteAttribute($this->getAttributesCollection(), 'integer');
        $database->deleteAttribute($this->getAttributesCollection(), 'bigint');
        $database->deleteAttribute($this->getAttributesCollection(), 'float');
        $database->deleteAttribute($this->getAttributesCollection(), 'boolean');
        $database->deleteAttribute($this->getAttributesCollection(), 'id');
        $database->deleteAttribute($this->getAttributesCollection(), 'varchar1');
        $database->deleteAttribute($this->getAttributesCollection(), 'varchar2');
        $database->deleteAttribute($this->getAttributesCollection(), 'text1');
        $database->deleteAttribute($this->getAttributesCollection(), 'mediumtext1');
        $database->deleteAttribute($this->getAttributesCollection(), 'longtext1');

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(17, $collection->attributes());
        $this->assertCount(0, $collection->indexes());

        // Delete Array
        $database->deleteAttribute($this->getAttributesCollection(), 'string_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'integer_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'float_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'boolean_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'varchar_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'text_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'mediumtext_list');
        $database->deleteAttribute($this->getAttributesCollection(), 'longtext_list');

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(9, $collection->attributes());

        // Delete default
        $database->deleteAttribute($this->getAttributesCollection(), 'string_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'integer_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'float_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'boolean_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'datetime_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'varchar_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'text_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'mediumtext_default');
        $database->deleteAttribute($this->getAttributesCollection(), 'longtext_default');

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(0, $collection->attributes());

        // Test for custom chars in ID
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'as_5dasdasdas', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'as5dasdasdas_', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: '.as5dasdasdas', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: '-as5dasdasdas', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'as-5dasdasdas', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'as5dasdasdas-', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: 'socialAccountForYoutubeSubscribersss', required: true));
        $database->createAttribute($this->getAttributesCollection(), Attribute::boolean(key: '5f058a89258075f058a89258075f058t9214', required: true));

        // Test non-shared tables duplicates throw duplicate
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'duplicate', size: 128, required: true));
        try {
            $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'duplicate', size: 128, required: true));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete attribute when column does not exist
        $database->createAttribute($this->getAttributesCollection(), Attribute::string(key: 'string1', size: 128, required: true));
        sleep(1);

        $this->assertEquals(true, $this->deleteColumn($this->getAttributesCollection(), 'string1'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $attributes = $collection->attributes();
        $attribute = end($attributes);
        $this->assertInstanceOf(Attribute::class, $attribute);
        $this->assertEquals('string1', $attribute->key);

        $database->deleteAttribute($this->getAttributesCollection(), 'string1');

        $collection = $database->getCollection($this->getAttributesCollection());
        $attributes = $collection->attributes();
        $attribute = end($attributes);
        $this->assertInstanceOf(Attribute::class, $attribute);
        $this->assertNotEquals('string1', $attribute->key);

        $collection = $database->getCollection($this->getAttributesCollection());
    }

    /**
     * Sets up the 'attributes' collection for tests that depend on testCreateDeleteAttribute.
     */
    private static bool $attributesCollectionFixtureInit = false;

    protected function initAttributesCollectionFixture(): void
    {
        if (self::$attributesCollectionFixtureInit) {
            return;
        }

        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: $this->getAttributesCollection()));

        self::$attributesCollectionFixtureInit = true;
    }

    public function testAttributeKeyWithSymbols(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'attributesWithKeys'));

        $database->createAttribute('attributesWithKeys', Attribute::string(key: 'key_with.sym$bols', size: 128, required: true));

        $document = $database->createDocument('attributesWithKeys', new Document([
            'key_with.sym$bols' => 'value',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
        ]));

        $this->assertEquals('value', $document->getAttribute('key_with.sym$bols'));

        $document = $database->getDocument('attributesWithKeys', $document->getId());

        $this->assertEquals('value', $document->getAttribute('key_with.sym$bols'));
    }

    public function testAttributeNamesWithDots(): void
    {
        if (! ($this->getDatabase()->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();
            return;
        }

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'dots.parent'));

        $database->createAttribute('dots.parent', Attribute::string(key: 'dots.name'));

        $document = $database->find('dots.parent', [
            Query::select(['dots.name']),
        ]);
        $this->assertEmpty($document);

        $database->createCollection(Collection::create(id: 'dots'));

        $database->createAttribute('dots', Attribute::string(key: 'name'));

        $database->createRelationship('dots.parent', Relationship::oneToOne(relatedCollection: 'dots'));

        $database->createDocument('dots.parent', new Document([
            '$id' => Id::custom('father'),
            'dots.name' => 'Bill clinton',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'dots' => [
                '$id' => Id::custom('child'),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ],
        ]));

        $documents = $database->find('dots.parent', [
            Query::select(['*']),
        ]);

        $this->assertEquals('Bill clinton', $documents[0]['dots.name']);
    }

    public function testDottedAttributeKeysFilterFindCountAndSumAlike(): void
    {
        $database = $this->getDatabase();
        $collection = $this->createDottedKeyCollection($database);

        $matching = [Query::equal('dots.name', ['v'])];
        $this->assertSame(['a', 'b'], $this->sortedIds($database->find($collection, $matching)));
        $this->assertSame(2, $database->count($collection, $matching));
        $this->assertSame(2, $database->count($collection, $matching, 10));
        $this->assertSame(1, $database->count($collection, $matching, 1));
        $this->assertSame(5, $database->sum($collection, 'dots.score', $matching));
        $this->assertSame(5, $database->sum($collection, 'dots.score', $matching, 10));
        $this->assertSame(10, $database->sum($collection, 'dots.score'));

        $missing = [Query::equal('dots.name', ['missing'])];
        $this->assertSame(0, $database->count($collection, $missing));
        $this->assertSame(0, $database->sum($collection, 'dots.score', $missing));

        $grouped = [Query::or([Query::equal('dots.name', ['w']), Query::greaterThan('dots.score', 2)])];
        $this->assertSame(['b', 'c'], $this->sortedIds($database->find($collection, $grouped)));
        $this->assertSame(2, $database->count($collection, $grouped));
        $this->assertSame(8, $database->sum($collection, 'dots.score', $grouped));

        $ordered = [Query::isNotNull('dots.name'), Query::orderDesc('dots.score')];
        $this->assertSame(3, $database->count($collection, $ordered));
        $this->assertSame(10, $database->sum($collection, 'dots.score', $ordered));

        $database->deleteCollection($collection);
    }

    public function testDottedAttributeKeysInExistsQueries(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();
        if ($adapter instanceof Memory || $adapter instanceof Redis) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->createDottedKeyCollection($database);

        $exists = [Query::exists(['dots.name'])];
        $present = $this->sortedIds($database->find($collection, $exists));
        $this->assertSame(['a', 'b', 'c'], \array_values(\array_intersect($present, ['a', 'b', 'c'])));
        $this->assertSame(\count($present), $database->count($collection, $exists));
        $this->assertSame(10, $database->sum($collection, 'dots.score', $exists));

        $notExists = [Query::notExists(['dots.name'])];
        $absent = $this->sortedIds($database->find($collection, $notExists));
        $this->assertSame([], \array_values(\array_intersect($absent, ['a', 'b', 'c'])));
        $this->assertSame(\count($absent), $database->count($collection, $notExists));
        $this->assertSame(4, \count($present) + \count($absent));

        $database->deleteCollection($collection);
    }

    public function testDottedAttributeKeysBesideJoinAliases(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->createDottedKeyCollection($database);
        $orders = $collection.'_orders';
        $database->createCollection(Collection::create(
            id: $orders,
            attributes: [
                Attribute::string(key: 'personId', size: 64, required: true),
                Attribute::integer(key: 'total', required: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument($orders, new Document(['$id' => 'o1', 'personId' => 'a', 'total' => 7]));

        $join = Query::join($orders, 'ord', [Query::on('$id', 'personId')]);
        $leftJoin = Query::leftJoin($orders, 'ord', [Query::on('$id', 'personId')]);

        $this->assertSame(['a'], $this->sortedIds($database->find($collection, [$join, Query::equal('dots.name', ['v'])])));
        $this->assertSame(1, $database->count($collection, [$join, Query::equal('dots.name', ['v'])]));
        $this->assertSame(7, $database->sum($collection, 'ord.total', [$join, Query::equal('dots.name', ['v'])]));
        $this->assertSame(2, $database->sum($collection, 'dots.score', [$join, Query::greaterThan('ord.total', 5)]));
        $this->assertSame(0, $database->count($collection, [$join, Query::greaterThan('ord.total', 7)]));

        $this->assertSame(['a'], $this->sortedIds($database->find($collection, [$join, Query::exists(['ord.$id'])])));
        $this->assertSame(1, $database->count($collection, [$join, Query::exists(['dots.name'])]));
        $this->assertSame(['b', 'c', 'd'], $this->sortedIds($database->find($collection, [$leftJoin, Query::notExists(['ord.$id'])])));
        $this->assertSame(3, $database->count($collection, [$leftJoin, Query::notExists(['ord.$createdAt'])]));

        $database->deleteCollection($orders);
        $database->deleteCollection($collection);
    }

    public function testDottedAttributeKeysInGroups(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->createDottedKeyCollection($database);

        $groups = $database->aggregate($collection, [
            Query::count('*', 'people'),
            Query::sum('dots.score', 'score'),
            Query::groupBy(['dots.name']),
            Query::exists(['dots.name']),
            Query::orderDesc('people'),
        ]);
        $this->assertSame([[2, 5], [1, 5]], \array_map(
            fn (array $group): array => [$this->aggregatedNumber($group, 'people'), $this->aggregatedNumber($group, 'score')],
            $groups,
        ));

        $filtered = $database->aggregate($collection, [
            Query::count('*', 'people'),
            Query::groupBy(['dots.name']),
            Query::equal('dots.name', ['w']),
        ]);
        $this->assertCount(1, $filtered);
        $this->assertSame(1, $this->aggregatedNumber($filtered[0], 'people'));

        $having = $database->aggregate($collection, [
            Query::count('*', 'people'),
            Query::groupBy(['dots.name']),
            Query::having([Query::greaterThan('people', 1)]),
        ]);
        $this->assertCount(1, $having);
        $this->assertSame(2, $this->aggregatedNumber($having[0], 'people'));

        $database->deleteCollection($collection);
    }

    /**
     * @param  array<string, mixed>  $group
     */
    private function aggregatedNumber(array $group, string $key): int
    {
        $value = $group[$key] ?? null;
        $this->assertIsNumeric($value, "The aggregate {$key} is a number");

        return (int) $value;
    }

    private function createDottedKeyCollection(Database $database): string
    {
        $collection = 'dotted_keys_'.\substr(\uniqid(), -6);
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [
                Attribute::string(key: 'dots.name', size: 64),
                Attribute::integer(key: 'dots.score'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        $database->createDocument($collection, new Document(['$id' => 'a', 'dots.name' => 'v', 'dots.score' => 2]));
        $database->createDocument($collection, new Document(['$id' => 'b', 'dots.name' => 'v', 'dots.score' => 3]));
        $database->createDocument($collection, new Document(['$id' => 'c', 'dots.name' => 'w', 'dots.score' => 5]));
        $database->createDocument($collection, new Document(['$id' => 'd']));

        return $collection;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function sortedIds(array $documents): array
    {
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $documents);
        \sort($ids);

        return $ids;
    }

    public function testUpdateAttributeDefault(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $collection = $this->getFlowersCollection();

        $flowers = $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'name', size: 128, required: true));
        $database->createAttribute($collection, Attribute::integer(key: 'inStock'));
        $database->createAttribute($collection, Attribute::string(key: 'date', size: 128));

        $database->createDocument($collection, new Document([
            '$id' => 'flowerWithDate',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Violet',
            'inStock' => 51,
            'date' => '2000-06-12 14:12:55.000',
        ]));

        $doc = $database->createDocument($collection, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily',
        ]));

        self::$flowersFixtureInit = true;

        $this->assertNull($doc->getAttribute('inStock'));

        $database->updateAttribute($this->getFlowersCollection(), 'inStock', new AttributeUpdate(default: 100));

        $doc = $database->createDocument($this->getFlowersCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Iris',
        ]));

        $this->assertIsNumeric($doc->getAttribute('inStock'));
        $this->assertEquals(100, $doc->getAttribute('inStock'));

        $database->updateAttribute($this->getFlowersCollection(), 'inStock', new AttributeUpdate(default: null));
    }

    public function testRenameAttribute(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $colors = $database->createCollection(Collection::create(id: $this->getColorsCollection()));
        $database->createAttribute($this->getColorsCollection(), Attribute::string(key: 'name', size: 128, required: true));
        $database->createAttribute($this->getColorsCollection(), Attribute::string(key: 'hex', size: 128, required: true));

        $database->createIndex($this->getColorsCollection(), Index::key(key: 'index1', attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc]));

        $database->createDocument($this->getColorsCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'black',
            'hex' => '#000000',
        ]));

        $database->renameAttribute($this->getColorsCollection(), 'name', 'verbose');

        $colors = $database->getCollection($this->getColorsCollection());
        $this->assertEquals('hex', $colors->attributes()[1]->key);
        $this->assertEquals('verbose', $colors->attributes()[0]->key);
        $this->assertCount(2, $colors->attributes());

        // Attribute in index is renamed automatically on adapter-level. What we need to check is if metadata is properly updated
        $this->assertEquals('verbose', $colors->indexes()[0]->attributes[0]);
        $this->assertCount(1, $colors->indexes());

        // Document should be there if adapter migrated properly
        $document = $database->findOne($this->getColorsCollection());
        $this->assertFalse($document->isEmpty());
        $this->assertEquals('black', $document->getAttribute('verbose'));
        $this->assertEquals('#000000', $document->getAttribute('hex'));
        $this->assertEquals(null, $document->getAttribute('name'));

        self::$colorsFixtureInit = true;
    }

    /**
     * Sets up the 'flowers' collection for tests that depend on testUpdateAttributeDefault.
     */
    private static bool $flowersFixtureInit = false;

    protected function initFlowersFixture(): void
    {
        if (self::$flowersFixtureInit) {
            return;
        }

        $database = $this->getDatabase();

        $collection = $this->getFlowersCollection();
        $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'name', size: 128, required: true));
        $database->createAttribute($collection, Attribute::integer(key: 'inStock'));
        $database->createAttribute($collection, Attribute::string(key: 'date', size: 128));

        $database->createDocument($collection, new Document([
            '$id' => 'flowerWithDate',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Violet',
            'inStock' => 51,
            'date' => '2000-06-12 14:12:55.000',
        ]));

        $database->createDocument($collection, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily',
        ]));

        self::$flowersFixtureInit = true;
    }

    public function testUpdateAttributeRequired(): void
    {
        $this->initFlowersFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->updateAttribute($this->getFlowersCollection(), 'inStock', new AttributeUpdate(required: true));

        $this->expectExceptionMessage('Invalid document structure: Missing required attribute "inStock"');

        $doc = $database->createDocument($this->getFlowersCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With Missing Stocks',
        ]));
    }

    public function testUnstorableColumnTypesAreRejectedUpFront(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'unstorable_column_types';
        $database->createCollection(Collection::create(id: $collection));

        foreach ([ColumnType::Json, ColumnType::Timestamp, ColumnType::BigSerial] as $type) {
            $message = 'Unknown attribute type: '.$type->value;
            $inline = $collection.'_'.$type->value;

            try {
                $database->createAttribute($collection, Attribute::fromArray(['key' => 'value', 'type' => $type]));
                $this->fail('Expected createAttribute() to reject '.$type->value);
            } catch (DatabaseException $error) {
                $this->assertStringContainsString($message, $error->getMessage());
            }

            try {
                $database->createCollection(Collection::create(id: $inline, attributes: [Attribute::fromArray(['key' => 'value', 'type' => $type])]));
                $this->fail('Expected createCollection() to reject '.$type->value);
            } catch (DatabaseException $error) {
                $this->assertStringContainsString($message, $error->getMessage());
            }

            $this->assertNull($database->findCollection($inline));
        }

        $this->assertSame([], $database->getCollection($collection)->attributes());

        $database->deleteCollection($collection);
    }

    public function testIdAttributeCanBeUpdated(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'id_attribute_update';
        $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::id(key: 'reference'));
        $database->createDocument($collection, new Document([
            '$id' => 'one',
            '$permissions' => [Permission::read(Role::any())],
            'reference' => '7',
        ]));

        $updated = $database->updateAttribute($collection, 'reference', new AttributeUpdate(key: 'target'));

        $this->assertSame('target', $updated->key);
        $this->assertSame(ColumnType::Id, $updated->type);
        $this->assertSame('7', $database->getDocument($collection, 'one')->getAttribute('target'));

        $database->deleteCollection($collection);
    }

    public function testRequiredOnlyChangeKeepsDatetimeColumnsWritable(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'datetime_required_relax';
        $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::datetime(key: 'at', required: true));
        $database->createDocument($collection, new Document([
            '$id' => 'one',
            '$permissions' => [Permission::read(Role::any())],
            'at' => '2024-01-01T00:00:00.000+00:00',
        ]));

        $updated = $database->updateAttribute($collection, 'at', new AttributeUpdate(required: false));
        $this->assertFalse($updated->required);

        $document = $database->createDocument($collection, new Document([
            '$id' => 'two',
            '$permissions' => [Permission::read(Role::any())],
            'at' => null,
        ]));
        $this->assertNull($document->getAttribute('at'));

        $database->deleteCollection($collection);
    }

    public function testUpdateAttributeFilter(): void
    {
        $this->initFlowersFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createAttribute($this->getFlowersCollection(), Attribute::string(key: 'cartModel', size: 2000));

        $doc = $database->createDocument($this->getFlowersCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With CartData',
            'inStock' => 50,
            'cartModel' => '{"color":"string","size":"number"}',
        ]));

        $this->assertIsString($doc->getAttribute('cartModel'));
        $this->assertEquals('{"color":"string","size":"number"}', $doc->getAttribute('cartModel'));

        $database->updateAttribute($this->getFlowersCollection(), 'cartModel', new AttributeUpdate(filters: [Filter::Json]));

        $doc = $database->getDocument($this->getFlowersCollection(), $doc->getId());
        $this->assertIsArray($doc->getAttribute('cartModel'));
        $this->assertCount(2, $doc->getAttribute('cartModel'));
        $this->assertEquals('string', $doc->getAttribute('cartModel')['color']);
        $this->assertEquals('number', $doc->getAttribute('cartModel')['size']);
    }

    public function testUpdateAttributeFormat(): void
    {
        $this->initFlowersFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Ensure cartModel attribute exists (created by testUpdateAttributeFilter in sequential mode)
        try {
            $database->createAttribute($this->getFlowersCollection(), Attribute::string(key: 'cartModel', size: 2000));
        } catch (\Exception $e) {
            // Already exists
        }

        $database->createAttribute($this->getFlowersCollection(), Attribute::integer(key: 'price'));

        $doc = $database->createDocument($this->getFlowersCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            '$id' => Id::custom('LiliPriced'),
            'name' => 'Lily Priced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 500,
        ]));

        $this->assertIsNumeric($doc->getAttribute('price'));
        $this->assertEquals(500, $doc->getAttribute('price'));

        Structure::addFormat('priceRange', $this->priceRangeFormat(...), ColumnType::Integer);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(format: new Format('priceRange', ['min' => 1, 'max' => 10000])));

        $this->expectExceptionMessage('Invalid document structure: Attribute "price" has invalid format. Value must be a valid range between 1 and 10,000');

        $doc = $database->createDocument($this->getFlowersCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily Overpriced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 15000,
        ]));
    }

    /**
     * Sets up the 'flowers' collection with price attribute and priceRange format
     * as testUpdateAttributeFormat would leave it.
     */
    private static bool $flowersWithPriceFixtureInit = false;

    protected function initFlowersWithPriceFixture(): void
    {
        if (self::$flowersWithPriceFixtureInit) {
            return;
        }

        $this->initFlowersFixture();

        $database = $this->getDatabase();

        // Add cartModel attribute (from testUpdateAttributeFilter)
        try {
            $database->createAttribute($this->getFlowersCollection(), Attribute::string(key: 'cartModel', size: 2000));
        } catch (\Exception $e) {
            // Already exists
        }

        // Add price attribute and set format (from testUpdateAttributeFormat)
        try {
            $database->createAttribute($this->getFlowersCollection(), Attribute::integer(key: 'price'));
        } catch (\Exception $e) {
            // Already exists
        }

        // Create LiliPriced document if it doesn't exist
        try {
            $database->createDocument($this->getFlowersCollection(), new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                '$id' => Id::custom('LiliPriced'),
                'name' => 'Lily Priced',
                'inStock' => 50,
                'cartModel' => '{}',
                'price' => 500,
            ]));
        } catch (\Exception $e) {
            // Already exists
        }

        Structure::addFormat('priceRange', $this->priceRangeFormat(...), ColumnType::Integer);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(format: new Format('priceRange', ['min' => 1, 'max' => 10000])));

        self::$flowersWithPriceFixtureInit = true;
    }

    public function testUpdateAttributeStructure(): void
    {
        $this->initFlowersWithPriceFixture();

        // TODO: When this becomes relevant, add many more tests (from all types to all types, chaging size up&down, switchign between array/non-array...

        Structure::addFormat('priceRangeNew', $this->priceRangeFormat(...), ColumnType::Integer);

        /** @var Database $database */
        $database = $this->getDatabase();

        // price attribute
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(null, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertEquals('priceRange', $attribute->format?->name);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute->format->options ?? []);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(default: 100));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(100, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertEquals('priceRange', $attribute->format?->name);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute->format->options ?? []);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(format: new Format('priceRangeNew', ['min' => 1, 'max' => 10000])));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(100, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertEquals('priceRangeNew', $attribute->format?->name);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute->format->options ?? []);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(format: new Format('priceRangeNew', ['min' => 1, 'max' => 999])));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(100, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertEquals('priceRangeNew', $attribute->format?->name);
        $this->assertEquals(['min' => 1, 'max' => 999], $attribute->format->options ?? []);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(format: new Format('priceRangeNew')));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(100, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertEquals('priceRangeNew', $attribute->format?->name);
        $this->assertEquals([], $attribute->format->options ?? []);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(format: null));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(100, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertNull($attribute->format);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(signed: false));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(false, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(100, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(false, $attribute->required);
        $this->assertNull($attribute->format);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(required: true));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::Integer, $attribute->type);
        $this->assertEquals(false, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(null, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(true, $attribute->required);
        $this->assertNull($attribute->format);

        $database->updateAttribute($this->getFlowersCollection(), 'price', new AttributeUpdate(type: ColumnType::String, size: Database::LENGTH_KEY, format: null));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[4];
        $this->assertEquals(ColumnType::String, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(255, $attribute->size);
        $this->assertEquals(null, $attribute->default);
        $this->assertEquals(false, $attribute->array);
        $this->assertEquals(true, $attribute->required);
        $this->assertNull($attribute->format);

        $attribute = $collection->attributes()[2];
        $this->assertEquals('date', $attribute->key);
        $this->assertEquals(ColumnType::String, $attribute->type);
        $this->assertEquals(null, $attribute->default);

        $database->updateAttribute($this->getFlowersCollection(), 'date', new AttributeUpdate(type: ColumnType::Datetime, size: 0, filters: [Filter::Datetime]));
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->attributes()[2];
        $this->assertEquals(ColumnType::Datetime, $attribute->type);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(null, $attribute->default);
        $this->assertEquals(false, $attribute->required);
        $this->assertEquals(false, $attribute->signed);
        $this->assertEquals(false, $attribute->array);
        $this->assertNull($attribute->format);

        $doc = $database->getDocument($this->getFlowersCollection(), 'LiliPriced');
        $this->assertIsString($doc->getAttribute('price'));
        $this->assertEquals('500', $doc->getAttribute('price'));

        $doc = $database->getDocument($this->getFlowersCollection(), 'flowerWithDate');
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $doc->getAttribute('date'));
    }

    public function testUpdateAttributeRename(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: 'rename_test'));

        $database->createAttribute('rename_test', Attribute::string(key: 'rename_me', size: 128, required: true));

        $doc = $database->createDocument('rename_test', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'rename_me' => 'string',
        ]));

        $this->assertEquals('string', $doc->getAttribute('rename_me'));

        // Create an index to check later
        $database->createIndex('rename_test', Index::key(key: 'renameIndexes', attributes: ['rename_me'], orders: [OrderDirection::Desc, OrderDirection::Desc]));

        $database->updateAttribute(
            collection: 'rename_test',
            key: 'rename_me',
            update: new AttributeUpdate(key: 'renamed'),
        );

        $doc = $database->getDocument('rename_test', $doc->getId());

        // Check the attribute was correctly renamed
        $this->assertEquals('string', $doc->getAttribute('renamed'));
        $this->assertArrayNotHasKey('rename_me', $doc);

        // Check we can update the document with the new key
        $doc->setAttribute('renamed', 'string2');
        $database->updateDocument('rename_test', $doc->getId(), $doc);

        $doc = $database->getDocument('rename_test', $doc->getId());
        $this->assertEquals('string2', $doc->getAttribute('renamed'));

        // Check collection
        $collection = $database->getCollection('rename_test');
        $this->assertEquals('renamed', $collection->attributes()[0]->key);
        $this->assertEquals('renamed', $collection->attributes()[0]->key);
        $this->assertEquals('renamed', $collection->indexes()[0]->attributes[0]);

        $supportsIdenticalIndexes = $database->getAdapter()->supports(Capability::IndexIdentical);

        try {
            // Check an update without a new key doesn't cause issues
            $database->updateAttribute(
                collection: 'rename_test',
                key: 'renamed',
                update: new AttributeUpdate(type: ColumnType::String),
            );

            if (! $supportsIdenticalIndexes) {
                $this->fail('Expected exception when getSupportForIdenticalIndexes=false but none was thrown');
            }
        } catch (Throwable $e) {
            if (! $supportsIdenticalIndexes) {
                $this->assertNotSame('', $e->getMessage());

                return; // Exit early if exception was expected
            } else {
                $this->fail('Unexpected exception when getSupportForIdenticalIndexes=true: '.$e->getMessage());
            }
        }

        $collection = $database->getCollection('rename_test');

        $this->assertEquals('renamed', $collection->attributes()[0]->key);
        $this->assertEquals('renamed', $collection->attributes()[0]->key);
        $this->assertEquals('renamed', $collection->indexes()[0]->attributes[0]);

        $doc = $database->getDocument('rename_test', $doc->getId());

        $this->assertEquals('string2', $doc->getAttribute('renamed'));
        $this->assertArrayNotHasKey('rename_me', $doc->getAttributes());

        // Check the metadata was correctly updated
        $attribute = $collection->attributes()[0];
        $this->assertEquals('renamed', $attribute->key);
        $this->assertEquals('renamed', $attribute->key);

        // Check the indexes were updated
        $index = $collection->indexes()[0];
        $this->assertEquals('renamed', $index->attributes[0]);
        $this->assertEquals(1, count($collection->indexes()));

        // Try and create new document with new key
        $doc = $database->createDocument('rename_test', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'renamed' => 'string',
        ]));

        $this->assertEquals('string', $doc->getAttribute('renamed'));

        // Make sure we can't create a new attribute with the old key
        try {
            $doc = $database->createDocument('rename_test', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'rename_me' => 'string',
            ]));
            $this->fail('Succeeded creating a document with old key after renaming the attribute');
        } catch (\Exception $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        // Check new key filtering
        $database->updateAttribute(
            collection: 'rename_test',
            key: 'renamed',
            update: new AttributeUpdate(key: 'renamed-test'),
        );

        $doc = $database->getDocument('rename_test', $doc->getId());

        $this->assertEquals('string', $doc->getAttribute('renamed-test'));
        $this->assertArrayNotHasKey('renamed', $doc->getAttributes());
    }

    /**
     * Sets up the 'colors' collection with renamed attributes as testRenameAttribute would leave it.
     */
    private static bool $colorsFixtureInit = false;

    protected function initColorsFixture(): void
    {
        if (self::$colorsFixtureInit) {
            return;
        }

        $database = $this->getDatabase();

        $collection = $this->getColorsCollection();
        $database->createCollection(Collection::create(id: $collection));
        $database->createAttribute($collection, Attribute::string(key: 'name', size: 128, required: true));
        $database->createAttribute($collection, Attribute::string(key: 'hex', size: 128, required: true));
        $database->createIndex($collection, Index::key(key: 'index1', attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc]));
        $database->createDocument($collection, new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'black',
            'hex' => '#000000',
        ]));
        $database->renameAttribute($collection, 'name', 'verbose');

        self::$colorsFixtureInit = true;
    }

    /**
     * @expectedException Exception
     */
    public function textRenameAttributeMissing(): void
    {
        $this->initColorsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->expectExceptionMessage('Attribute not found');
        $database->renameAttribute($this->getColorsCollection(), 'name2', 'name3');
    }

    /**
     * @expectedException Exception
     */
    public function testRenameAttributeExisting(): void
    {
        $this->initColorsFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $this->expectExceptionMessage('Attribute name already used');
        $database->renameAttribute($this->getColorsCollection(), 'verbose', 'hex');
    }

    public function testExceptionWidthLimit(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if ($database->getAdapter()->limits()->documentSize === 0) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $attributes = [];

        $attributes[] = Attribute::string(key: 'varchar_16000', size: 16000, required: true);

        $attributes[] = Attribute::string(key: 'varchar_200', size: 200, required: true);

        try {
            $database->createCollection(Collection::create(id: 'attributes_row_size', attributes: $attributes));
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Document size limit of 65535 exceeded. Cannot create collection.', $e->getMessage());
        }

        /**
         * Remove last attribute
         */
        array_pop($attributes);

        $collection = $database->createCollection(Collection::create(id: 'attributes_row_size', attributes: $attributes));

        $attribute = Attribute::string(key: 'breaking', size: 200, required: true);

        try {
            $database->checkAttribute($collection->getId(), $attribute);
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertStringContainsString('Row width limit reached. Cannot create new attribute.', $e->getMessage());
            $this->assertStringContainsString('bytes but the maximum is 65535 bytes', $e->getMessage());
            $this->assertStringContainsString('Reduce the size of existing attributes or remove some attributes to free up space.', $e->getMessage());
        }

        try {
            $database->createAttribute($collection->getId(), Attribute::string(key: 'breaking', size: 200, required: true));
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertStringContainsString('Row width limit reached. Cannot create new attribute.', $e->getMessage());
            $this->assertStringContainsString('bytes but the maximum is 65535 bytes', $e->getMessage());
            $this->assertStringContainsString('Reduce the size of existing attributes or remove some attributes to free up space.', $e->getMessage());
        }
    }

    public function testUpdateAttributeSize(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::AttributeResizing)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(Collection::create(id: 'resize_test'));

        $database->createAttribute('resize_test', Attribute::string(key: 'resize_me', size: 128, required: true));
        $document = $database->createDocument('resize_test', new Document([
            '$id' => Id::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'resize_me' => $this->createRandomString(128),
        ]));

        // Go up in size

        // 0-16381 to 16382-65535
        $document = $this->updateStringAttributeSize(16382, $document);

        // 16382-65535 to 65536-16777215
        $document = $this->updateStringAttributeSize(65536, $document);

        // 65536-16777216 to PHP_INT_MAX or adapter limit
        $document = $this->updateStringAttributeSize(16777217, $document);

        // Test going down in size with data that is too big (Expect Failure)
        try {
            $database->updateAttribute('resize_test', 'resize_me', new AttributeUpdate(type: ColumnType::String, size: 128, required: true));
            $this->fail('Succeeded updating attribute size to smaller size with data that is too big');
        } catch (TruncateException $e) {
        }

        // Test going down in size when data isn't too big.
        $database->updateDocument('resize_test', $document->getId(), $document->setAttribute('resize_me', $this->createRandomString(128)));
        $database->updateAttribute('resize_test', 'resize_me', new AttributeUpdate(type: ColumnType::String, size: 128, required: true));

        // VARCHAR -> VARCHAR Truncation Test
        $database->updateAttribute('resize_test', 'resize_me', new AttributeUpdate(type: ColumnType::String, size: 1000, required: true));
        $database->updateDocument('resize_test', $document->getId(), $document->setAttribute('resize_me', $this->createRandomString(1000)));

        try {
            $database->updateAttribute('resize_test', 'resize_me', new AttributeUpdate(type: ColumnType::String, size: 128, required: true));
            $this->fail('Succeeded updating attribute size to smaller size with data that is too big');
        } catch (TruncateException $e) {
        }

        if ($database->getAdapter()->limits()->indexLength > 0) {
            $length = intval($database->getAdapter()->limits()->indexLength / 2);

            $database->createAttribute('resize_test', Attribute::string(key: 'attr1', size: $length, required: true));
            $database->createAttribute('resize_test', Attribute::string(key: 'attr2', size: $length, required: true));

            /**
             * No index length provided, we are able to validate
             */
            $database->createIndex('resize_test', Index::key(key: 'index1', attributes: ['attr1', 'attr2']));

            try {
                $database->updateAttribute('resize_test', 'attr1', new AttributeUpdate(type: ColumnType::String, size: 5000));
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->limits()->indexLength, $e->getMessage());
            }

            $database->deleteIndex('resize_test', 'index1');

            /**
             * Index lengths are provided, We are able to validate
             * Index $length === attr1, $length === attr2, so $length is removed, so we are able to validate
             */
            $database->createIndex('resize_test', Index::key(key: 'index1', attributes: ['attr1', 'attr2'], lengths: [$length, $length]));

            $collection = $database->getCollection('resize_test');
            $indexes = $collection->indexes();
            $this->assertEquals(null, $indexes[0]->lengths[0]);
            $this->assertEquals(null, $indexes[0]->lengths[1]);

            try {
                $database->updateAttribute('resize_test', 'attr1', new AttributeUpdate(type: ColumnType::String, size: 5000));
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->limits()->indexLength, $e->getMessage());
            }

            $database->deleteIndex('resize_test', 'index1');

            /**
             * Index lengths are provided
             * We are able to increase size because index length remains 50
             */
            $database->createIndex('resize_test', Index::key(key: 'index1', attributes: ['attr1', 'attr2'], lengths: [50, 50]));

            $collection = $database->getCollection('resize_test');
            $indexes = $collection->indexes();
            $this->assertEquals(50, $indexes[0]->lengths[0]);
            $this->assertEquals(50, $indexes[0]->lengths[1]);

            $database->updateAttribute('resize_test', 'attr1', new AttributeUpdate(type: ColumnType::String, size: 5000));
        }
    }

    public function testEncryptAttributes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        // Add custom encrypt filter
        $database->addFilter(
            'encrypt',
            function (mixed $value): string {
                if (! \is_scalar($value) && $value !== null) {
                    throw new \InvalidArgumentException('Encrypted filter input must be scalar or null');
                }

                return json_encode([
                    'data' => base64_encode((string) $value),
                    'method' => 'base64',
                    'version' => 'v1',
                ]) ?: throw new \RuntimeException('Failed to encode encrypted filter input');
            },
            function (mixed $value): ?string {
                if (is_null($value)) {
                    return null;
                }
                if (! \is_string($value)) {
                    throw new \InvalidArgumentException('Encrypted filter value must be a string');
                }

                $value = json_decode($value, true);
                if (! \is_array($value) || ! \is_string($value['data'] ?? null)) {
                    throw new \InvalidArgumentException('Encrypted filter payload is invalid');
                }

                $decoded = base64_decode($value['data'], true);
                if ($decoded === false) {
                    throw new \InvalidArgumentException('Encrypted filter payload is not valid base64');
                }

                return $decoded;
            }
        );

        $col = $database->createCollection(Collection::create(id: __FUNCTION__));
        $this->assertNotSame('', $col->getId());

        $database->createAttribute($col->getId(), Attribute::string(key: 'title', required: true));
        $database->createAttribute($col->getId(), Attribute::string(key: 'encrypt', size: 128, required: true, filters: ['encrypt']));

        $database->createDocument($col->getId(), new Document([
            'title' => 'Sample Title',
            'encrypt' => 'secret',
        ]));
        // query against encrypt
        try {
            $queries = [Query::equal('encrypt', ['test'])];
            $doc = $database->find($col->getId(), $queries);
            $this->fail('Queried against encrypt field. Failed to throw exeception.');
        } catch (Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
        }

        try {
            $queries = [Query::equal('title', ['test'])];
            $database->find($col->getId(), $queries);
        } catch (Throwable) {
            $this->fail('Should not have thrown error');
        }
    }

    /**
     * A filter can build its value by querying rather than transforming the stored one — that is
     * what the subQuery filters in Appwrite do, listing a child collection per document. Reading a
     * document without selecting such an attribute must not run it: the value is dropped anyway,
     * and it is the filter, not the value, that costs the query.
     */
    public function testFilterNotAppliedWhenAttributeNotSelected(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $calls = 0;

        $database->addFilter(
            'subQueryProbe',
            fn (mixed $value) => null, // stores nothing, like a subQuery filter
            function (mixed $value) use (&$calls) {
                $calls++;
                return ['fanned', 'out'];
            }
        );

        $database->createCollection(Collection::create(id: 'filterSelect'));
        $database->createAttribute('filterSelect', Attribute::string(key: 'plain', size: 128));
        $database->createAttribute('filterSelect', Attribute::string(key: 'kids', size: 128, filters: ['subQueryProbe']));

        $database->createDocument('filterSelect', new Document([
            '$id' => 'doc1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plain' => 'x',
        ]));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1');
        $this->assertEquals(1, $calls);
        $this->assertEquals(['fanned', 'out'], $document->getAttribute('kids'));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1', [Query::select(['$id', 'plain'])]);
        $this->assertEquals(0, $calls);
        $this->assertNull($document->getAttribute('kids'));
        $this->assertEquals('x', $document->getAttribute('plain'));

        // Selecting it explicitly, and selecting everything, both still decode it.
        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1', [Query::select(['$id', 'kids'])]);
        $this->assertEquals(1, $calls);
        $this->assertEquals(['fanned', 'out'], $document->getAttribute('kids'));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1', [Query::select(['*'])]);
        $this->assertEquals(1, $calls);
        $this->assertEquals(['fanned', 'out'], $document->getAttribute('kids'));

        // find() decodes through the same path, once per document returned.
        $calls = 0;
        $documents = $database->find('filterSelect', [Query::select(['$id', 'plain'])]);
        $this->assertCount(1, $documents);
        $this->assertEquals(0, $calls);
        $this->assertNull($documents[0]->getAttribute('kids'));

        $calls = 0;
        $documents = $database->find('filterSelect');
        $this->assertCount(1, $documents);
        $this->assertEquals(1, $calls);
        $this->assertEquals(['fanned', 'out'], $documents[0]->getAttribute('kids'));
    }

    public function updateStringAttributeSize(int $size, Document $document): Document
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->updateAttribute('resize_test', 'resize_me', new AttributeUpdate(type: ColumnType::String, size: $size, required: true));

        $document = $document->setAttribute('resize_me', $this->createRandomString($size));

        $database->updateDocument('resize_test', $document->getId(), $document);
        $checkDoc = $database->getDocument('resize_test', $document->getId());

        $this->assertEquals($document->getAttribute('resize_me'), $checkDoc->getAttribute('resize_me'));
        $resized = $checkDoc->getAttribute('resize_me');
        $this->assertIsString($resized);
        $this->assertEquals($size, strlen($resized));

        return $checkDoc;
    }

    /**
     * @throws AuthorizationException
     * @throws DuplicateException
     * @throws ConflictException
     * @throws LimitException
     * @throws StructureException
     */
    public function testArrayAttribute(): void
    {
        $this->getDatabase()->getAuthorization()->addRole(Role::any()->toString());

        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'json';
        $permissions = [Permission::read(Role::any())];

        $database->createCollection(Collection::create(id: $collection, permissions: [
            Permission::create(Role::any()),
        ]));

        $database->createAttribute($collection, Attribute::boolean(key: 'booleans', required: true, array: true));

        $database->createAttribute($collection, Attribute::string(key: 'names', array: true));

        $database->createAttribute($collection, Attribute::string(key: 'cards', size: 5000, array: true));

        $database->createAttribute($collection, Attribute::integer(key: 'numbers', array: true));

        $database->createAttribute($collection, Attribute::integer(key: 'age', signed: false));

        $database->createAttribute($collection, Attribute::string(key: 'tv_show', size: $database->getAdapter()->limits()->indexLength - 68));

        $database->createAttribute($collection, Attribute::string(key: 'short', size: 5, array: true));

        $database->createAttribute($collection, Attribute::string(key: 'pref', size: 16384, filters: [Filter::Json]));

        try {
            $database->createDocument($collection, new Document([]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertEquals('Invalid document structure: Missing required attribute "booleans"', $e->getMessage());
            }
        }

        $database->updateAttribute($collection, 'booleans', new AttributeUpdate(required: false));

        $doc = $database->getCollection($collection);
        $attribute = $doc->attributes()[0];
        $this->assertEquals(ColumnType::Boolean, $attribute->type);
        $this->assertEquals(true, $attribute->signed);
        $this->assertEquals(0, $attribute->size);
        $this->assertEquals(null, $attribute->default);
        $this->assertEquals(true, $attribute->array);
        $this->assertEquals(false, $attribute->required);

        try {
            $database->createDocument($collection, new Document([
                'short' => ['More than 5 size'],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertEquals('Invalid document structure: Attribute "short[\'0\']" has invalid type. Value must be a valid string and no longer than 5 chars', $e->getMessage());
            }
        }

        try {
            $database->createDocument($collection, new Document([
                'names' => ['Joe', 100],
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertEquals('Invalid document structure: Attribute "names[\'1\']" has invalid type. Value must be a valid string and no longer than 255 chars', $e->getMessage());
            }
        }

        try {
            $database->createDocument($collection, new Document([
                'age' => 1.5,
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertEquals('Invalid document structure: Attribute "age" has invalid type. Value must be a valid unsigned 32-bit integer between 0 and 4,294,967,295', $e->getMessage());
            }
        }

        try {
            $database->createDocument($collection, new Document([
                'age' => -100,
            ]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertEquals('Invalid document structure: Attribute "age" has invalid type. Value must be a valid unsigned 32-bit integer between 0 and 4,294,967,295', $e->getMessage());
            }
        }

        $database->createDocument($collection, new Document([
            '$id' => 'id1',
            '$permissions' => $permissions,
            'booleans' => [false],
            'names' => ['Joe', 'Antony', '100'],
            'numbers' => [0, 100, 1000, -1],
            'age' => 41,
            'tv_show' => 'Everybody Loves Raymond',
            'pref' => [
                'fname' => 'Joe',
                'lname' => 'Baiden',
                'age' => 80,
                'male' => true,
            ],
        ]));

        $document = $database->getDocument($collection, 'id1');

        $this->assertEquals(false, $document->getArray('booleans')[0]);
        $this->assertEquals('Antony', $document->getArray('names')[1]);
        $this->assertEquals(100, $document->getArray('numbers')[1]);

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            /**
             * Functional index dependency cannot be dropped or rename
             */
            $database->createIndex($collection, Index::key(key: 'idx_cards', attributes: ['cards'], lengths: [100]));
        }

        if ($database->getAdapter()->supports(Capability::IndexArrayCast)) {
            /**
             * Delete attribute
             */
            try {
                $database->deleteAttribute($collection, 'cards');
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertInstanceOf(DependencyException::class, $e);
                $this->assertEquals("Attribute can't be deleted or renamed because it is used in an index", $e->getMessage());
            }

            /**
             * Rename attribute
             */
            try {
                $database->renameAttribute($collection, 'cards', 'cards_new');
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertInstanceOf(DependencyException::class, $e);
                $this->assertEquals("Attribute can't be deleted or renamed because it is used in an index", $e->getMessage());
            }

            /**
             * Update attribute
             */
            try {
                $database->updateAttribute($collection, key: 'cards', update: new AttributeUpdate(key: 'cards_new'));
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertInstanceOf(DependencyException::class, $e);
                $this->assertEquals("Attribute can't be deleted or renamed because it is used in an index", $e->getMessage());
            }

        } else {
            $database->renameAttribute($collection, 'cards', 'cards_new');
            $database->deleteAttribute($collection, 'cards_new');
        }

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            try {
                $database->createIndex($collection, Index::fulltext(key: 'indx', attributes: ['names']));
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $this->fail('Failed to throw exception');
                }
            } catch (Throwable $e) {
                if ($database->getAdapter()->supports(Capability::IndexFulltext)) {
                    $this->assertEquals('"Fulltext" index is forbidden on array attributes', $e->getMessage());
                } else {
                    $this->assertEquals('Fulltext index is not supported', $e->getMessage());
                }
            }

            try {
                $database->createIndex($collection, Index::key(key: 'indx', attributes: ['numbers', 'names'], lengths: [100, 100]));
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $this->fail('Failed to throw exception');
                }
            } catch (Throwable $e) {
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $this->assertEquals('An index may only contain one array attribute', $e->getMessage());
                } else {
                    $this->assertEquals('Index already exists', $e->getMessage());
                }
            }
        }

        $database->createAttribute($collection, Attribute::string(key: 'long_size', size: 2000, array: true));

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes) && $database->getAdapter()->limits()->indexLength > 0) {
                // If getMaxIndexLength() > 0 We clear length for array attributes
                $database->createIndex($collection, Index::key(key: 'indx1', attributes: ['long_size']));
                $database->deleteIndex($collection, 'indx1');
                $database->createIndex($collection, Index::key(key: 'indx2', attributes: ['long_size'], lengths: [1000]));

                try {
                    $database->createIndex($collection, Index::key(key: 'indx_numbers', attributes: ['tv_show', 'numbers'])); // [700, 255]
                    $this->fail('Failed to throw exception');
                } catch (Throwable $e) {
                    $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->limits()->indexLength, $e->getMessage());
                }
            }

            try {
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $database->createIndex($collection, Index::key(key: 'indx4', attributes: ['age', 'names'], lengths: [10, 255]));
                    $this->fail('Failed to throw exception');
                }
            } catch (Throwable $e) {
                $this->assertEquals('Cannot set a length on "integer" attributes', $e->getMessage());
            }

            $database->createIndex($collection, Index::key(key: 'indx6', attributes: ['age', 'names'], lengths: [null, 999]));
            $database->createIndex($collection, Index::key(key: 'indx7', attributes: ['age', 'booleans'], lengths: [0, 999]));
        }

        try {
            $database->find($collection, [
                Query::equal('names', ['Joe']),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid query: Cannot query equal on attribute "names" because it is an array.', $e->getMessage());
        }

        try {
            $database->find($collection, [
                new Query(Method::Contains, 'age', [10]),
            ]);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertEquals('Invalid query: Cannot query contains on attribute "age" because it is not an array, string, or object.', $e->getMessage());
        }

        $documents = $database->find($collection, [
            Query::isNull('long_size'),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            Query::containsString('tv_show', ['love']),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            new Query(Method::Contains, 'names', ['Jake', 'Joe']),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            new Query(Method::Contains, 'numbers', [-1, 0, 999]),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            new Query(Method::Contains, 'booleans', [false, true]),
        ]);
        $this->assertCount(1, $documents);

        // Regular like query on primitive json string data
        $documents = $database->find($collection, [
            Query::containsString('pref', ['Joe']),
        ]);
        $this->assertCount(1, $documents);

        // containsAny tests — should behave identically to contains

        $documents = $database->find($collection, [
            Query::containsAny('tv_show', ['love']),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            Query::containsAny('names', ['Jake', 'Joe']),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            Query::containsAny('numbers', [-1, 0, 999]),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            Query::containsAny('booleans', [false, true]),
        ]);
        $this->assertCount(1, $documents);

        $documents = $database->find($collection, [
            Query::containsAny('pref', ['Joe']),
        ]);
        $this->assertCount(1, $documents);

        // containsAny with no matching values
        $documents = $database->find($collection, [
            Query::containsAny('names', ['Jake', 'Unknown']),
        ]);
        $this->assertCount(0, $documents);

        // containsAll tests on array attributes

        // All values present in names array
        $documents = $database->find($collection, [
            Query::containsAll('names', ['Joe', 'Antony']),
        ]);
        $this->assertCount(1, $documents);

        // One value missing from names array
        $documents = $database->find($collection, [
            Query::containsAll('names', ['Joe', 'Jake']),
        ]);
        $this->assertCount(0, $documents);

        // All values present in numbers array
        $documents = $database->find($collection, [
            Query::containsAll('numbers', [0, 100, -1]),
        ]);
        $this->assertCount(1, $documents);

        // One value missing from numbers array
        $documents = $database->find($collection, [
            Query::containsAll('numbers', [0, 999]),
        ]);
        $this->assertCount(0, $documents);

        // Single value containsAll — should match
        $documents = $database->find($collection, [
            Query::containsAll('booleans', [false]),
        ]);
        $this->assertCount(1, $documents);

        // Boolean value not present
        $documents = $database->find($collection, [
            Query::containsAll('booleans', [true]),
        ]);
        $this->assertCount(0, $documents);
    }

    public function testCreateDatetime(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'datetime'));
        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $database->createAttribute('datetime', Attribute::datetime(key: 'date', required: true));
            $database->createAttribute('datetime', Attribute::datetime(key: 'date2'));
        }

        try {
            $database->createDocument('datetime', new Document([
                'date' => ['2020-01-01'], // array
            ]));
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->fail('Failed to throw exception');
            }
        } catch (Exception $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertInstanceOf(StructureException::class, $e);
            }
        }

        $doc = $database->createDocument('datetime', new Document([
            '$id' => Id::custom('id1234'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'date' => DateTime::now(),
        ]));

        $createdAt = $doc->getCreatedAt();
        $updatedAt = $doc->getUpdatedAt();
        $this->assertNotNull($createdAt);
        $this->assertNotNull($updatedAt);
        $this->assertEquals(29, strlen($createdAt));
        $this->assertEquals(29, strlen($updatedAt));
        $this->assertEquals('+00:00', substr($createdAt, -6));
        $this->assertEquals('+00:00', substr($updatedAt, -6));
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getCreatedAt());
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getUpdatedAt());

        $document = $database->getDocument('datetime', 'id1234');

        $min = $database->getAdapter()->limits()->minDateTime;
        $max = $database->getAdapter()->limits()->maxDateTime;
        $dateValidator = new DatetimeValidator($min, $max);
        $this->assertEquals(null, $document->getAttribute('date2'));
        $this->assertEquals(true, $dateValidator->isValid($document->getAttribute('date')));
        $this->assertEquals(false, $dateValidator->isValid($document->getAttribute('date2')));

        $documents = $database->find('datetime', [
            Query::greaterThan('date', '1975-12-06 10:00:00+01:00'),
            Query::lessThan('date', '2030-12-06 10:00:00-01:00'),
        ]);
        $this->assertEquals(1, count($documents));

        $documents = $database->find('datetime', [
            Query::greaterThan('$createdAt', '1975-12-06 11:00:00.000'),
        ]);
        $this->assertCount(1, $documents);

        try {
            $database->createDocument('datetime', new Document([
                '$id' => 'datenew1',
                'date' => '1975-12-06 00:00:61', // 61 seconds is invalid,
            ]));
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->fail('Failed to throw exception');
            }
        } catch (Exception $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertInstanceOf(StructureException::class, $e);
            }
        }

        try {
            $database->createDocument('datetime', new Document([
                'date' => '+055769-02-14T17:56:18.000Z',
            ]));
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->fail('Failed to throw exception');
            }
        } catch (Exception $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertInstanceOf(StructureException::class, $e);
            }
        }

        $invalidDates = [
            '+055769-02-14T17:56:18.000Z1',
            '1975-12-06 00:00:61',
            '16/01/2024 12:00:00AM',
        ];

        foreach ($invalidDates as $date) {
            try {
                $database->find('datetime', [
                    Query::equal('$createdAt', [$date]),
                ]);
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertTrue($e instanceof QueryException);
                $this->assertEquals('Invalid query: Query value is invalid for attribute "$createdAt"', $e->getMessage());
            }

            try {
                $database->find('datetime', [
                    Query::equal('date', [$date]),
                ]);
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $this->fail('Failed to throw exception');
                }
            } catch (Throwable $e) {
                $this->assertTrue($e instanceof QueryException);
                $this->assertEquals('Invalid query: Query value is invalid for attribute "date"', $e->getMessage());
            }
        }

        $validDates = [
            '2024-12-25 09:00:21.891119',
            '2024-12-31 00:00:00.000000',
        ];

        foreach ($validDates as $date) {
            $docs = $database->find('datetime', [
                Query::equal('$createdAt', [$date]),
            ]);
            $this->assertCount(0, $docs);

            $docs = $database->find('datetime', [
                Query::equal('date', [$date]),
            ]);
            $this->assertCount(0, $docs);

            /**
             * Test convertQueries on nested queries
             */
            $docs = $database->find('datetime', [
                Query::or([
                    Query::equal('$createdAt', [$date]),
                    Query::equal('date', [$date]),
                ]),
            ]);
            $this->assertCount(0, $docs);
        }
    }

    public function testCreateDatetimeAddingAutoFilter(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'datetime_auto_filter';

        // The same attribute through both public creation paths: createCollection() takes it
        // inline, createAttribute() adds it to a collection that already exists. Both have to
        // attach the datetime filter, or the same value written through one of them is stored
        // and returned differently from the other.
        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::datetime(key: 'inline')],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));

        $offset = '2024-01-02T03:04:05.000+05:00';
        $database->createDocument($collection, new Document([
            Document::ID => 'offset',
            'inline' => $offset,
        ]));
        $database->createDocument($collection, new Document([
            Document::ID => 'utc',
            'inline' => '2024-01-01 22:04:05.000',
        ]));

        // The filter normalises the offset on the way in and restores one on the way out, so
        // the two spellings of the same instant come back as one string, carrying its zone.
        // createCollection() runs no validator over its attributes, so a missing filter here
        // costs the normalisation silently rather than refusing the write.
        $this->assertSame(
            '2024-01-01T22:04:05.000+00:00',
            $database->getDocument($collection, 'offset')->getAttribute('inline')
        );
        $this->assertSame(
            $database->getDocument($collection, 'offset')->getAttribute('inline'),
            $database->getDocument($collection, 'utc')->getAttribute('inline')
        );

        $database->createAttribute($collection, Attribute::datetime(key: 'added'));

        $database->createDocument($collection, new Document([
            Document::ID => 'both',
            'inline' => $offset,
            'added' => $offset,
        ]));

        $both = $database->getDocument($collection, 'both');
        $this->assertSame($both->getAttribute('inline'), $both->getAttribute('added'));

        $attributes = $database->getCollection($collection)->attributes();
        $this->assertCount(2, $attributes);

        foreach ($attributes as $attribute) {
            $this->assertSame([ColumnType::Datetime->value], $attribute->filters);
        }

        $database->deleteCollection($collection);
    }

    public function testCreateAttributesAddingAutoFilter(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collection = 'datetime_batch_auto_filter';

        $database->createCollection(Collection::create(
            id: $collection,
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));

        $database->createAttributes($collection, [Attribute::datetime(key: 'batch')]);

        $database->createDocument($collection, new Document([
            Document::ID => 'offset',
            'batch' => '2024-01-02T03:04:05.000+05:00',
        ]));

        $this->assertSame(
            '2024-01-01T22:04:05.000+00:00',
            $database->getDocument($collection, 'offset')->getAttribute('batch')
        );

        $database->deleteCollection($collection);
    }

    public function testCreateAttributesBigIntIgnoresSizeMetadata(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collectionName = 'bigint_ignores_size_limit';
        $database->createCollection(Collection::create(id: $collectionName));

        $attributes = [Attribute::fromArray(['key' => 'foo', 'type' => ColumnType::BigInteger, 'size' => 9999])];

        $database->createAttributes($collectionName, $attributes);

        $collection = $database->getCollection($collectionName);
        $attrs = $collection->attributes();
        $this->assertCount(1, $attrs);
        $attribute = $attrs[0];
        $this->assertSame('foo', $attribute->key);
        $this->assertNull($attribute->size);

        $database->updateAttribute($collectionName, 'foo', new AttributeUpdate(type: ColumnType::BigInteger, size: 1));
        $collection = $database->getCollection($collectionName);
        $attrs = $collection->attributes();
        $this->assertCount(1, $attrs);
        $this->assertNull($attrs[0]->size);
    }

    public function testCreateAttributesBigIntValidationSignedUnsignedAndMetadata(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collectionName = 'bigint_attr_validation';
        $database->createCollection(Collection::create(id: $collectionName));

        $database->createAttribute(
            $collectionName,
            Attribute::bigInteger(key: 'signed_bigint'),
        );
        $database->createAttribute(
            $collectionName,
            Attribute::bigInteger(key: 'unsigned_bigint', signed: false),
        );

        $collection = $database->getCollection($collectionName);
        $attributes = $collection->attributes();

        $signedAttribute = null;
        $unsignedAttribute = null;
        foreach ($attributes as $attribute) {
            if ($attribute->key === 'signed_bigint') {
                $signedAttribute = $attribute;
            }
            if ($attribute->key === 'unsigned_bigint') {
                $unsignedAttribute = $attribute;
            }
        }

        $this->assertInstanceOf(Attribute::class, $signedAttribute);
        $this->assertInstanceOf(Attribute::class, $unsignedAttribute);
        $this->assertTrue($signedAttribute->signed);
        $this->assertFalse($unsignedAttribute->signed);
        $this->assertNull($signedAttribute->size);
        $this->assertNull($unsignedAttribute->size);

        $largeUnsignedAttribute = [Attribute::bigInteger(key: 'unsigned_bigint_large', default: '18446744073709551615', signed: false)];
        if ($database->getAdapter()->supports(Capability::UnsignedBigInt)) {
            $database->createAttributes($collectionName, $largeUnsignedAttribute);
        } else {
            try {
                $database->createAttributes($collectionName, $largeUnsignedAttribute);
                $this->fail('Expected unsupported unsigned bigint default to be rejected');
            } catch (DatabaseException $exception) {
                $this->assertStringContainsString('does not match given type bigint', $exception->getMessage());
            }
        }
    }

    public function testBigIntegerAttributesPersistTheBigintSpelling(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $collectionName = 'bigint_persisted_spelling';
        $database->createCollection(Collection::create(
            id: $collectionName,
            attributes: [Attribute::bigInteger(key: 'inline')],
        ));
        $database->createAttribute($collectionName, Attribute::bigInteger(key: 'single'));
        $expected = ['inline' => 'bigint', 'single' => 'bigint'];

        $database->createAttributes($collectionName, [Attribute::bigInteger(key: 'batch')]);
        $expected['batch'] = 'bigint';

        $database->updateAttribute($collectionName, 'single', new AttributeUpdate(required: true));

        $stored = $database->skipFilters(fn (): Document => $database->getAuthorization()->skip(
            fn (): Document => $database->getDocument(Database::METADATA, $collectionName),
        ));
        $storedAttributes = $stored->getAttribute('attributes');
        $this->assertIsString($storedAttributes);

        /** @var list<array<string, mixed>> $decoded */
        $decoded = \json_decode($storedAttributes, true, flags: JSON_THROW_ON_ERROR);
        $types = [];
        foreach ($decoded as $attribute) {
            $key = $attribute['key'] ?? null;
            $this->assertIsString($key);
            $types[$key] = $attribute['type'] ?? null;
        }
        $this->assertSame($expected, $types);

        foreach ($database->getCollection($collectionName)->attributes() as $attribute) {
            $this->assertSame(ColumnType::BigInteger, $attribute->type, $attribute->key);
        }
    }

    public function testCreateAttributesSuccessMultiple(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        $attributes = [Attribute::string(key: 'a', size: 10), Attribute::integer(key: 'b')];

        $database->createAttributes(__FUNCTION__, $attributes);

        $collection = $database->getCollection(__FUNCTION__);
        $attrs = $collection->attributes();
        $this->assertCount(2, $attrs);
        $this->assertEquals('a', $attrs[0]->key);
        $this->assertEquals('b', $attrs[1]->key);

        $doc = $database->createDocument(__FUNCTION__, new Document([
            'a' => 'foo',
            'b' => 123,
        ]));

        $this->assertEquals('foo', $doc->getAttribute('a'));
        $this->assertEquals(123, $doc->getAttribute('b'));
    }

    public function testCreateAttributesDelete(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        $attributes = [Attribute::string(key: 'a', size: 10), Attribute::integer(key: 'b')];

        $database->createAttributes(__FUNCTION__, $attributes);

        $collection = $database->getCollection(__FUNCTION__);
        $attrs = $collection->attributes();
        $this->assertCount(2, $attrs);
        $this->assertEquals('a', $attrs[0]->key);
        $this->assertEquals('b', $attrs[1]->key);

        $database->deleteAttribute(__FUNCTION__, 'a');

        $collection = $database->getCollection(__FUNCTION__);
        $attrs = $collection->attributes();
        $this->assertCount(1, $attrs);
        $this->assertEquals('b', $attrs[0]->key);
    }

    public function testStringTypeAttributes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: 'stringTypes'));

        // Create attributes with different string types
        $database->createAttribute('stringTypes', Attribute::varchar(key: 'varchar_field', default: 'default varchar'));
        $database->createAttribute('stringTypes', Attribute::text(key: 'text_field', size: 65535));
        $database->createAttribute('stringTypes', Attribute::mediumText(key: 'mediumtext_field', size: 16777215));
        $database->createAttribute('stringTypes', Attribute::longText(key: 'longtext_field', size: 4294967295));

        // Test with array types
        $database->createAttribute('stringTypes', Attribute::varchar(key: 'varchar_array', size: 128, array: true));
        $database->createAttribute('stringTypes', Attribute::text(key: 'text_array', size: 65535, array: true));

        $collection = $database->getCollection('stringTypes');
        $this->assertCount(6, $collection->attributes());

        // Test VARCHAR with valid data
        $doc1 = $database->createDocument('stringTypes', new Document([
            '$id' => Id::custom('doc1'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'varchar_field' => 'This is a varchar field with 255 max length',
            'text_field' => \str_repeat('a', 1000),
            'mediumtext_field' => \str_repeat('b', 100000),
            'longtext_field' => \str_repeat('c', 1000000),
        ]));

        $this->assertEquals('This is a varchar field with 255 max length', $doc1->getAttribute('varchar_field'));
        $this->assertEquals(\str_repeat('a', 1000), $doc1->getAttribute('text_field'));
        $this->assertEquals(\str_repeat('b', 100000), $doc1->getAttribute('mediumtext_field'));
        $this->assertEquals(\str_repeat('c', 1000000), $doc1->getAttribute('longtext_field'));

        // Test VARCHAR with default value
        $doc2 = $database->createDocument('stringTypes', new Document([
            '$id' => Id::custom('doc2'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $this->assertEquals('default varchar', $doc2->getAttribute('varchar_field'));
        $this->assertNull($doc2->getAttribute('text_field'));

        // Test array types
        $doc3 = $database->createDocument('stringTypes', new Document([
            '$id' => Id::custom('doc3'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'varchar_array' => ['test1', 'test2', 'test3'],
            'text_array' => [\str_repeat('x', 1000), \str_repeat('y', 2000)],
        ]));

        $this->assertEquals(['test1', 'test2', 'test3'], $doc3->getAttribute('varchar_array'));
        $this->assertEquals([\str_repeat('x', 1000), \str_repeat('y', 2000)], $doc3->getAttribute('text_array'));

        // Test VARCHAR size constraint (should fail) - only for adapters that support attributes
        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            try {
                $database->createDocument('stringTypes', new Document([
                    '$id' => Id::custom('doc4'),
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::create(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'varchar_field' => \str_repeat('a', 256), // Too long for VARCHAR(255)
                ]));
                $this->fail('Failed to throw exception for VARCHAR size violation');
            } catch (Exception $e) {
                $this->assertInstanceOf(StructureException::class, $e);
            }

            // Test TEXT size constraint (should fail)
            try {
                $database->createDocument('stringTypes', new Document([
                    '$id' => Id::custom('doc5'),
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::create(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'text_field' => \str_repeat('a', 65536), // Too long for TEXT(65535)
                ]));
                $this->fail('Failed to throw exception for TEXT size violation');
            } catch (Exception $e) {
                $this->assertInstanceOf(StructureException::class, $e);
            }
        }

        // Test querying by VARCHAR field
        $database->createIndex('stringTypes', Index::key(key: 'varchar_index', attributes: ['varchar_field']));

        $results = $database->find('stringTypes', [
            Query::equal('varchar_field', ['This is a varchar field with 255 max length']),
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals('doc1', $results[0]->getId());

        // Test updating VARCHAR field
        $database->updateDocument('stringTypes', 'doc1', new Document([
            '$id' => 'doc1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'varchar_field' => 'Updated varchar value',
        ]));

        $updatedDoc = $database->getDocument('stringTypes', 'doc1');
        $this->assertEquals('Updated varchar value', $updatedDoc->getAttribute('varchar_field'));
    }

    #[DataProvider('invalidDefaultValues')]
    public function testInvalidDefaultValues(ColumnType $type, mixed $default): void
    {
        $database = $this->getDatabase();
        $collection = 'bad_default_'.uniqid();

        $database->createCollection(Collection::create(id: $collection));

        try {
            $database->createAttribute($collection, Attribute::fromArray(['key' => 'bad_default', 'type' => $type, 'size' => 256, 'default' => $default]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertStringContainsString('does not match given type', $e->getMessage());
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testAttributeAndIndexKeysAreCaseInsensitive(): void
    {
        $database = $this->getDatabase();
        $collection = 'case_insensitive_'.uniqid();

        $database->createCollection(Collection::create(id: $collection));

        $database->createAttribute($collection, Attribute::string(key: 'caseSensitive', size: 128, required: true));

        try {
            $database->createAttribute($collection, Attribute::string(key: 'CaseSensitive', size: 128, required: true));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $database->createIndex($collection, Index::key(key: 'key_caseSensitive', attributes: ['caseSensitive'], lengths: [128]));

        try {
            $database->createIndex($collection, Index::key(key: 'key_CaseSensitive', attributes: ['caseSensitive'], lengths: [128]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $database->deleteCollection($collection);
    }

    public function testUnknownFormat(): void
    {
        $database = $this->getDatabase();
        $collection = 'unknown_format_'.uniqid();

        $database->createCollection(Collection::create(id: $collection));

        try {
            $database->createAttribute($collection, Attribute::string(key: 'bad_format', size: 256, required: true, format: new Format('url')));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testExceptionAttributeLimit(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if ($adapter->limits()->attributes === 0) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $limit = $adapter->limits()->attributes - $adapter->limits()->defaultAttributes;

        $attributes = [];
        for ($i = 0; $i <= $limit; $i++) {
            $attributes[] = Attribute::integer(key: "attr_{$i}");
        }

        try {
            $database->createCollection(Collection::create(id: 'attributes_limit', attributes: $attributes));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertSame('Attribute limit of '.$adapter->limits()->attributes.' exceeded. Cannot create collection.', $e->getMessage());
        }

        array_pop($attributes);

        $collection = $database->createCollection(Collection::create(id: 'attributes_limit', attributes: $attributes));

        $attribute = Attribute::string(key: 'breaking', size: 100, required: true);

        try {
            $database->checkAttribute($collection->getId(), $attribute);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertStringContainsString('Column limit reached. Cannot create new attribute.', $e->getMessage());
            $this->assertStringContainsString('Remove some attributes to free up space.', $e->getMessage());
        }

        try {
            $database->createAttribute($collection->getId(), $attribute);
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertStringContainsString('Column limit reached. Cannot create new attribute.', $e->getMessage());
            $this->assertStringContainsString('Remove some attributes to free up space.', $e->getMessage());
        }

        $database->deleteCollection('attributes_limit');
    }

    public function testWidthLimit(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();

        if ($adapter->limits()->documentSize === 0) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $database->createCollection(Collection::create(id: 'width_limit'));

        $init = $adapter->getAttributeWidth($collection);
        $this->assertSame(1067, $init);

        $width = fn (Attribute $attribute): int => $adapter->getAttributeWidth(Collection::create(id: 'width_limit', attributes: [$attribute])) - $init;

        $this->assertSame(401, $width(Attribute::string(key: 'varchar_100', size: 100)), 'VARCHAR(100) is 100 * 4 bytes plus a 1 byte length');
        $this->assertSame(20, $width(Attribute::string(key: 'json', size: 100, array: true)), 'An array is stored externally, only the pointer counts');
        $this->assertSame(20, $width(Attribute::string(key: 'text', size: 20000)), 'A string past the varchar limit is stored externally');
        $this->assertSame(8, $width(Attribute::integer(key: 'bigint', width: IntegerWidth::Bits64)));
        $this->assertSame(7, $width(Attribute::datetime(key: 'date')));

        $database->deleteCollection('width_limit');
    }

    public function testCreateAttributesEmpty(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        try {
            $database->createAttributes(__FUNCTION__, []);
            $this->fail('Expected DatabaseException not thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
        }
    }

    public function testCreateAttributesMissingKey(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        try {
            $database->createAttributes(__FUNCTION__, [Attribute::string(key: '', size: 10)]);
            $this->fail('Expected DatabaseException not thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertSame('Missing attribute key', $e->getMessage());
        }
    }

    public function testCreateAttributesDuplicateMetadata(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));
        $database->createAttribute(__FUNCTION__, Attribute::string(key: 'dup', size: 10));

        try {
            $database->createAttributes(__FUNCTION__, [Attribute::string(key: 'dup', size: 10)]);
            $this->fail('Expected DuplicateException not thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }
    }

    public function testCreateAttributesInvalidFormat(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        try {
            $database->createAttributes(__FUNCTION__, [Attribute::string(key: 'foo', size: 10, format: new Format('nonexistent'))]);
            $this->fail('Expected DatabaseException not thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
        }
    }

    public function testCreateAttributesDefaultOnRequired(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        try {
            $database->createAttributes(__FUNCTION__, [Attribute::string(key: 'foo', size: 10, required: true, default: 'bar')]);
            $this->fail('Expected DatabaseException not thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
            $this->assertSame('Cannot set a default value for a required attribute', $e->getMessage());
        }
    }

    public function testCreateAttributesStringSizeLimit(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        $max = $database->getAdapter()->limits()->string;

        try {
            $database->createAttributes(__FUNCTION__, [Attribute::string(key: 'foo', size: $max + 1)]);
            $this->fail('Expected DatabaseException not thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(DatabaseException::class, $e);
        }
    }

    public function testWideIntegerBecomesBits64(): void
    {
        $database = $this->getDatabase();

        $database->createCollection(Collection::create(id: __FUNCTION__));

        $limit = (int) ($database->getAdapter()->limits()->integer / 2);

        $created = $database->createAttributes(__FUNCTION__, [Attribute::fromArray(['key' => 'foo', 'type' => ColumnType::Integer, 'size' => $limit + 1])]);

        $this->assertSame(IntegerWidth::Bits64, $created[0]->width());
        $this->assertSame(IntegerWidth::Bits64, $database->getCollection(__FUNCTION__)->attributes()[0]->width());
    }

    public function testCreateAttributesSkipsAColumnThatExistsOnlyInTheSchema(): void
    {
        $database = $this->getDatabase();

        $collection = 'schemaOnlyColumn';
        $database->createCollection(Collection::create(id: $collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));
        $database->getAdapter()->createAttribute($collection, Attribute::string(key: 'b', size: 64));

        $database->createAttributes($collection, [
            Attribute::integer(key: 'a'),
            Attribute::string(key: 'b', size: 64),
        ]);

        $this->assertSame(['a', 'b'], \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection($collection)->attributes(),
        ));

        $database->createDocument($collection, new Document([Document::ID => 'one', 'a' => 1, 'b' => 'kept']));
        $document = $database->getDocument($collection, 'one');
        $this->assertSame(1, $document->getAttribute('a'));
        $this->assertSame('kept', $document->getAttribute('b'));

        $database->deleteCollection($collection);
    }

    public function testSharedTablesNeverDropAnotherTenantsColumn(): void
    {
        $database = $this->getDatabase();

        if (! $database->hasSharedTables()) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $originalTenant = $database->getTenant();
        $integerTenants = $database->getIdAttributeType() === ColumnType::Integer;
        $first = $integerTenants ? 301 : 'tenant_301';
        $second = $integerTenants ? 302 : 'tenant_302';
        $collection = 'sharedColumn_'.\uniqid();
        $definition = Collection::create(id: $collection, permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false);

        try {
            $database->setTenant($first);
            $database->createCollection($definition);
            $database->createAttribute($collection, Attribute::integer(key: 'age'));
            $database->createDocument($collection, new Document([Document::ID => 'first', 'age' => 7]));

            $database->setTenant($second);
            $database->createCollection($definition);

            $adapter = $database->getAdapter();
            if ($adapter->supports(Capability::SchemaIntrospection)) {
                try {
                    $database->createAttribute($collection, Attribute::string(key: 'age', size: 64));
                    $this->fail('A column another tenant stores with another type must be refused');
                } catch (DuplicateException $error) {
                    $this->assertSame('Attribute exists in the shared table with another type', $error->getMessage());
                }

                $database->createAttribute($collection, Attribute::integer(key: 'age'));
                $this->assertSame(['age'], \array_map(
                    static fn (Attribute $attribute): string => $attribute->key,
                    $database->getCollection($collection)->attributes(),
                ));
            } else {
                $database->createAttribute($collection, Attribute::string(key: 'age', size: 64));
            }

            $database->setTenant($first);
            $this->assertSame(7, $database->getDocument($collection, 'first')->getAttribute('age'));
        } finally {
            foreach ([$second, $first] as $tenant) {
                try {
                    $database->setTenant($tenant)->deleteCollection($collection);
                } catch (Throwable) {
                }
            }
            $database->setTenant($originalTenant);
        }
    }

    public function testRenameAttributeCompletesAnOrphanedRename(): void
    {
        $database = $this->getDatabase();
        $adapter = $database->getAdapter();
        $schemaAttributes = $adapter->supports(Capability::SchemaIntrospection);

        if (! $schemaAttributes && ! $adapter instanceof SQL) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'orphanedRename';
        $database->createCollection(Collection::create(id: $collection, attributes: [
            Attribute::string(key: 'before', size: 64),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ], documentSecurity: false));
        $database->createDocument($collection, new Document([Document::ID => 'one', 'before' => 'kept']));

        try {
            $adapter->renameAttribute($collection, 'before', 'after');

            $database->renameAttribute($collection, 'before', 'after');
            $this->assertSame(['after'], $this->getAttributeKeys($database, $collection));

            $document = $database->getDocument($collection, 'one');
            $this->assertSame('kept', $document->getAttribute('after'));
            $this->assertFalse($document->offsetExists('before'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    /**
     * @return list<string>
     */
    private function getAttributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection($collection)->attributes(),
        );
    }

    public function testSharedTablesTenantsRenameAnAttributeInTurn(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            foreach ([$first, $second] as $tenant) {
                $database->setTenant($tenant);
                $database->renameAttribute($collection, 'age', 'years');
                $this->assertSame('fullName', $database->updateAttribute($collection, 'name', new AttributeUpdate(size: 128, key: 'fullName'))->key);
            }

            foreach ([$first, $second] as $index => $tenant) {
                $database->setTenant($tenant);
                $document = $database->getDocument($collection, 'user');

                $this->assertSame(['years', 'fullName', 'nick'], $this->getAttributeKeys($database, $collection));
                $this->assertSame(($index + 1) * 10, $document->getAttribute('years'));
                $this->assertSame('name'.$index, $document->getAttribute('fullName'));
                $this->assertFalse($document->offsetExists('age'));
                $this->assertFalse($document->offsetExists('name'));

                $database->updateDocument($collection, 'user', new Document(['years' => ($index + 1) * 100]));
                $this->assertSame(($index + 1) * 100, $database->getDocument($collection, 'user')->getAttribute('years'));
            }
        });
    }

    public function testSharedTablesTenantsRenameAnIndexInTurn(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            foreach ([$first, $second] as $tenant) {
                $database->setTenant($tenant);
                $database->renameIndex($collection, 'byAge', 'ageIndex');
            }

            $this->assertTenantsFindByTheRenamedIndex($database, $collection, $first, $second);
        });
    }

    public function testSharedTablesALaterTenantRenamesAnIndexFirst(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            foreach ([$second, $first] as $tenant) {
                $database->setTenant($tenant);
                $database->renameIndex($collection, 'byAge', 'ageIndex');
            }

            $this->assertTenantsFindByTheRenamedIndex($database, $collection, $first, $second);
        });
    }

    public function testSharedTablesRenameOfAnIndexNoTenantHasInTheSchemaFails(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            $database->setTenant($first);
            $database->getAdapter()->deleteIndex($collection, 'byAge');

            $database->setTenant($second);
            if ($database->getAdapter() instanceof SQLite) {
                $database->renameIndex($collection, 'byAge', 'ageIndex');
                $this->assertSame(['ageIndex'], $this->getIndexKeys($database, $collection));

                return;
            }

            try {
                $database->renameIndex($collection, 'byAge', 'ageIndex');
                $this->fail('A rename no tenant\'s index backs must fail');
            } catch (DatabaseException $error) {
                $this->assertStringStartsWith("Failed to rename index 'byAge' to 'ageIndex'", $error->getMessage());
            }

            $this->assertSame(['byAge'], $this->getIndexKeys($database, $collection));
        });
    }

    public function testSharedTablesRenameOfAnIndexOnlyAnotherTenantHasFails(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            if (! $database->getAdapter() instanceof Postgres) {
                $this->expectNotToPerformAssertions();

                return;
            }

            foreach ([$first, $second] as $tenant) {
                $database->setTenant($tenant);
                $database->createIndex($collection, Index::key(key: 'byName', attributes: ['name']));
            }

            $database->setTenant($first);
            $database->getAdapter()->deleteIndex($collection, 'byName');

            try {
                $database->renameIndex($collection, 'byName', 'nameIndex');
                $this->fail('A rename backed only by another tenant\'s index must fail');
            } catch (DatabaseException $error) {
                $this->assertStringStartsWith("Failed to rename index 'byName' to 'nameIndex'", $error->getMessage());
            }

            $this->assertContains('byName', $this->getIndexKeys($database, $collection));
        });
    }

    private function assertTenantsFindByTheRenamedIndex(Database $database, string $collection, int|string ...$tenants): void
    {
        foreach (\array_values($tenants) as $index => $tenant) {
            $database->setTenant($tenant);

            $this->assertSame(['ageIndex'], $this->getIndexKeys($database, $collection));
            $this->assertSame(['user'], \array_map(
                static fn (Document $document): string => $document->getId(),
                $database->find($collection, [Query::equal('age', [($index + 1) * 10])]),
            ));
        }
    }

    public function testSharedTablesRenameOfAMissingAttributeIsNotFound(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            $database->setTenant($first);
            $database->renameAttribute($collection, 'age', 'years');

            $database->setTenant($second);
            try {
                $database->renameAttribute($collection, 'missing', 'found');
                $this->fail('Renaming an attribute the collection does not have must be refused');
            } catch (NotFoundException $error) {
                $this->assertSame('Attribute not found', $error->getMessage());
            }

            $this->assertSame(['age', 'name', 'nick'], $this->getAttributeKeys($database, $collection));
        });
    }

    public function testSharedTablesRenameOntoAnotherTenantsAttributeIsRefused(): void
    {
        $this->runSharedRename(function (Database $database, string $collection, int|string $first, int|string $second): void {
            $database->setTenant($second);
            $database->createAttribute($collection, Attribute::string(key: 'title', size: 32));
            $database->updateDocument($collection, 'user', new Document(['title' => 'title1']));

            $database->setTenant($first);
            try {
                $database->renameAttribute($collection, 'nick', 'title');
                $this->fail('A rename onto another tenant\'s column must be refused while the old column holds values');
            } catch (DuplicateException $error) {
                $this->assertSame('Attribute already exists', $error->getMessage());
            }

            try {
                $database->updateAttribute($collection, 'nick', new AttributeUpdate(key: 'title'));
                $this->fail('A key update onto another tenant\'s column must be refused while the old column holds values');
            } catch (DuplicateException $error) {
                $this->assertSame('Attribute already exists', $error->getMessage());
            }

            $this->assertSame(['age', 'name', 'nick'], $this->getAttributeKeys($database, $collection));
            $this->assertSame('nick0', $database->getDocument($collection, 'user')->getAttribute('nick'));

            $database->setTenant($second);
            $document = $database->getDocument($collection, 'user');
            $this->assertSame('nick1', $document->getAttribute('nick'));
            $this->assertSame('title1', $document->getAttribute('title'));
        });
    }

    /**
     * @param  callable(Database, string, int|string, int|string): void  $scenario
     */
    private function runSharedRename(callable $scenario): void
    {
        $database = $this->getDatabase();

        if (! $database->hasSharedTables() || ! $database->getAdapter() instanceof SQL) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $originalTenant = $database->getTenant();
        $integerTenants = $database->getIdAttributeType() === ColumnType::Integer;
        $tenants = $integerTenants ? [501, 502] : ['tenant_501', 'tenant_502'];
        $collection = 'sharedRename_'.\uniqid();
        $definition = Collection::create(id: $collection, attributes: [
            Attribute::integer(key: 'age'),
            Attribute::string(key: 'name', size: 64),
            Attribute::string(key: 'nick', size: 64),
        ], indexes: [
            Index::key(key: 'byAge', attributes: ['age']),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ], documentSecurity: false);

        try {
            foreach ($tenants as $index => $tenant) {
                $database->setTenant($tenant);
                $database->createCollection($definition);
                $database->createDocument($collection, new Document([
                    Document::ID => 'user',
                    'age' => ($index + 1) * 10,
                    'name' => 'name'.$index,
                    'nick' => 'nick'.$index,
                ]));
            }

            $scenario($database, $collection, ...$tenants);
        } finally {
            foreach (\array_reverse($tenants) as $tenant) {
                try {
                    $database->setTenant($tenant)->deleteCollection($collection);
                } catch (Throwable) {
                }
            }
            $database->setTenant($originalTenant);
        }
    }
}
