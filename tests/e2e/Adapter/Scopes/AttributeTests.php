<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Dependency as DependencyException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Structure;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;
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
     * Using phpunit dataProviders to check that all these combinations of types/defaults throw exceptions
     * https://phpunit.de/manual/3.7/en/writing-tests-for-phpunit.html#writing-tests-for-phpunit.data-providers
     *
     * @return array<array<bool|float|int|string>>
     */
    public function invalidDefaultValues(): array
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

        $database->createCollection($this->getAttributesCollection());

        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string1', type: ColumnType::String, size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string2', type: ColumnType::String, size: 16382 + 1, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string3', type: ColumnType::String, size: 65535 + 1, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string4', type: ColumnType::String, size: 16777215 + 1, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'integer', type: ColumnType::Integer, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'bigint', type: ColumnType::Integer, size: 8, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'float', type: ColumnType::Double, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'boolean', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'id', type: ColumnType::Id, size: 0, required: true)));

        // New string types
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'varchar1', type: ColumnType::Varchar, size: 255, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'varchar2', type: ColumnType::Varchar, size: 128, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'text1', type: ColumnType::Text, size: 65535, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'mediumtext1', type: ColumnType::MediumText, size: 16777215, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'longtext1', type: ColumnType::LongText, size: 4294967295, required: true)));

        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'id_index', type: IndexType::Key, attributes: ['id'])));
        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'string1_index', type: IndexType::Key, attributes: ['string1'])));
        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'string2_index', type: IndexType::Key, attributes: ['string2'], lengths: [255])));
        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'multi_index', type: IndexType::Key, attributes: ['string1', 'string2', 'string3'], lengths: [128, 128, 128])));
        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'varchar1_index', type: IndexType::Key, attributes: ['varchar1'])));
        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'varchar2_index', type: IndexType::Key, attributes: ['varchar2'])));
        $this->assertEquals(true, $database->createIndex($this->getAttributesCollection(), new Index(key: 'text1_index', type: IndexType::Key, attributes: ['text1'], lengths: [255])));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(14, $collection->getAttribute('attributes'));
        $this->assertCount(7, $collection->getAttribute('indexes'));

        // Array
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string_list', type: ColumnType::String, size: 128, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'integer_list', type: ColumnType::Integer, size: 0, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'float_list', type: ColumnType::Double, size: 0, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'boolean_list', type: ColumnType::Boolean, size: 0, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'varchar_list', type: ColumnType::Varchar, size: 128, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'text_list', type: ColumnType::Text, size: 65535, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'mediumtext_list', type: ColumnType::MediumText, size: 16777215, required: true, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'longtext_list', type: ColumnType::LongText, size: 4294967295, required: true, default: null, signed: true, array: true)));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(22, $collection->getAttribute('attributes'));

        // Default values
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string_default', type: ColumnType::String, size: 256, required: false, default: 'test')));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'integer_default', type: ColumnType::Integer, size: 0, required: false, default: 1)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'float_default', type: ColumnType::Double, size: 0, required: false, default: 1.5)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'boolean_default', type: ColumnType::Boolean, size: 0, required: false, default: false)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'datetime_default', type: ColumnType::Datetime, size: 0, required: false, default: '2000-06-12T14:12:55.000+00:00', signed: true, array: false, format: null, formatOptions: [], filters: ['datetime'])));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'varchar_default', type: ColumnType::Varchar, size: 255, required: false, default: 'varchar default')));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'text_default', type: ColumnType::Text, size: 65535, required: false, default: 'text default')));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'mediumtext_default', type: ColumnType::MediumText, size: 16777215, required: false, default: 'mediumtext default')));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'longtext_default', type: ColumnType::LongText, size: 4294967295, required: false, default: 'longtext default')));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(31, $collection->getAttribute('attributes'));

        // Delete
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string1'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string2'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string3'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string4'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'integer'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'bigint'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'float'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'boolean'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'id'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'varchar1'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'varchar2'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'text1'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'mediumtext1'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'longtext1'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(17, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // Delete Array
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'integer_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'float_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'boolean_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'varchar_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'text_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'mediumtext_list'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'longtext_list'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(9, $collection->getAttribute('attributes'));

        // Delete default
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'integer_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'float_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'boolean_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'datetime_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'varchar_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'text_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'mediumtext_default'));
        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'longtext_default'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $this->assertCount(0, $collection->getAttribute('attributes'));

        // Test for custom chars in ID
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'as_5dasdasdas', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'as5dasdasdas_', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: '.as5dasdasdas', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: '-as5dasdasdas', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'as-5dasdasdas', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'as5dasdasdas-', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'socialAccountForYoutubeSubscribersss', type: ColumnType::Boolean, size: 0, required: true)));
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: '5f058a89258075f058a89258075f058t9214', type: ColumnType::Boolean, size: 0, required: true)));

        // Test non-shared tables duplicates throw duplicate
        $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'duplicate', type: ColumnType::String, size: 128, required: true));
        try {
            $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'duplicate', type: ColumnType::String, size: 128, required: true));
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete attribute when column does not exist
        $this->assertEquals(true, $database->createAttribute($this->getAttributesCollection(), new Attribute(key: 'string1', type: ColumnType::String, size: 128, required: true)));
        sleep(1);

        $this->assertEquals(true, $this->deleteColumn($this->getAttributesCollection(), 'string1'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $attributes = $collection->getAttribute('attributes');
        $attribute = end($attributes);
        $this->assertEquals('string1', $attribute->getId());

        $this->assertEquals(true, $database->deleteAttribute($this->getAttributesCollection(), 'string1'));

        $collection = $database->getCollection($this->getAttributesCollection());
        $attributes = $collection->getAttribute('attributes');
        $attribute = end($attributes);
        $this->assertNotEquals('string1', $attribute->getId());

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

        $database->createCollection($this->getAttributesCollection());

        self::$attributesCollectionFixtureInit = true;
    }

    public function testAttributeKeyWithSymbols(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('attributesWithKeys');

        $this->assertEquals(true, $database->createAttribute('attributesWithKeys', new Attribute(key: 'key_with.sym$bols', type: ColumnType::String, size: 128, required: true)));

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
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('dots.parent');

        $this->assertTrue($database->createAttribute('dots.parent', new Attribute(key: 'dots.name', type: ColumnType::String, size: 255, required: false)));

        $document = $database->find('dots.parent', [
            Query::select(['dots.name']),
        ]);
        $this->assertEmpty($document);

        $database->createCollection('dots');

        $this->assertTrue($database->createAttribute('dots', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: false)));

        $database->createRelationship(new Relationship(collection: 'dots.parent', relatedCollection: 'dots', type: RelationType::OneToOne));

        $database->createDocument('dots.parent', new Document([
            '$id' => ID::custom('father'),
            'dots.name' => 'Bill clinton',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'dots' => [
                '$id' => ID::custom('child'),
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

    public function testUpdateAttributeDefault(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $collection = $this->getFlowersCollection();

        $flowers = $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute($collection, new Attribute(key: 'inStock', type: ColumnType::Integer, size: 0, required: false));
        $database->createAttribute($collection, new Attribute(key: 'date', type: ColumnType::String, size: 128, required: false));

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

        $database->updateAttributeDefault($this->getFlowersCollection(), 'inStock', 100);

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

        $database->updateAttributeDefault($this->getFlowersCollection(), 'inStock', null);
    }

    public function testRenameAttribute(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $colors = $database->createCollection($this->getColorsCollection());
        $database->createAttribute($this->getColorsCollection(), new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute($this->getColorsCollection(), new Attribute(key: 'hex', type: ColumnType::String, size: 128, required: true));

        $database->createIndex($this->getColorsCollection(), new Index(key: 'index1', type: IndexType::Key, attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc->value]));

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

        $attribute = $database->renameAttribute($this->getColorsCollection(), 'name', 'verbose');

        $this->assertTrue($attribute);

        $colors = $database->getCollection($this->getColorsCollection());
        $this->assertEquals('hex', $colors->getAttribute('attributes')[1]['$id']);
        $this->assertEquals('verbose', $colors->getAttribute('attributes')[0]['$id']);
        $this->assertCount(2, $colors->getAttribute('attributes'));

        // Attribute in index is renamed automatically on adapter-level. What we need to check is if metadata is properly updated
        $this->assertEquals('verbose', $colors->getAttribute('indexes')[0]->getAttribute('attributes')[0]);
        $this->assertCount(1, $colors->getAttribute('indexes'));

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
        $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute($collection, new Attribute(key: 'inStock', type: ColumnType::Integer, size: 0, required: false));
        $database->createAttribute($collection, new Attribute(key: 'date', type: ColumnType::String, size: 128, required: false));

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

        $database->updateAttributeRequired($this->getFlowersCollection(), 'inStock', true);

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

    public function testUpdateAttributeFilter(): void
    {
        $this->initFlowersFixture();

        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createAttribute($this->getFlowersCollection(), new Attribute(key: 'cartModel', type: ColumnType::String, size: 2000, required: false));

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

        $database->updateAttributeFilters($this->getFlowersCollection(), 'cartModel', ['json']);

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
            $database->createAttribute($this->getFlowersCollection(), new Attribute(key: 'cartModel', type: ColumnType::String, size: 2000, required: false));
        } catch (\Exception $e) {
            // Already exists
        }

        $database->createAttribute($this->getFlowersCollection(), new Attribute(key: 'price', type: ColumnType::Integer, size: 0, required: false));

        $doc = $database->createDocument($this->getFlowersCollection(), new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            '$id' => ID::custom('LiliPriced'),
            'name' => 'Lily Priced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 500,
        ]));

        $this->assertIsNumeric($doc->getAttribute('price'));
        $this->assertEquals(500, $doc->getAttribute('price'));

        Structure::addFormat('priceRange', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];

            return new Range($min, $max);
        }, ColumnType::Integer->value);

        $database->updateAttributeFormat($this->getFlowersCollection(), 'price', 'priceRange');
        $database->updateAttributeFormatOptions($this->getFlowersCollection(), 'price', ['min' => 1, 'max' => 10000]);

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
            $database->createAttribute($this->getFlowersCollection(), new Attribute(key: 'cartModel', type: ColumnType::String, size: 2000, required: false));
        } catch (\Exception $e) {
            // Already exists
        }

        // Add price attribute and set format (from testUpdateAttributeFormat)
        try {
            $database->createAttribute($this->getFlowersCollection(), new Attribute(key: 'price', type: ColumnType::Integer, size: 0, required: false));
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
                '$id' => ID::custom('LiliPriced'),
                'name' => 'Lily Priced',
                'inStock' => 50,
                'cartModel' => '{}',
                'price' => 500,
            ]));
        } catch (\Exception $e) {
            // Already exists
        }

        Structure::addFormat('priceRange', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];

            return new Range($min, $max);
        }, ColumnType::Integer->value);

        $database->updateAttributeFormat($this->getFlowersCollection(), 'price', 'priceRange');
        $database->updateAttributeFormatOptions($this->getFlowersCollection(), 'price', ['min' => 1, 'max' => 10000]);

        self::$flowersWithPriceFixtureInit = true;
    }

    public function testUpdateAttributeStructure(): void
    {
        $this->initFlowersWithPriceFixture();

        // TODO: When this becomes relevant, add many more tests (from all types to all types, chaging size up&down, switchign between array/non-array...

        Structure::addFormat('priceRangeNew', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];

            return new Range($min, $max);
        }, ColumnType::Integer->value);

        /** @var Database $database */
        $database = $this->getDatabase();

        // price attribute
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', default: 100);
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', format: 'priceRangeNew');
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRangeNew', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', format: '');
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', formatOptions: ['min' => 1, 'max' => 999]);
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 999], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', formatOptions: []);
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', signed: false);
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', required: true);
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute($this->getFlowersCollection(), 'price', type: ColumnType::String, size: Database::LENGTH_KEY, format: '');
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('string', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(255, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $collection->getAttribute('attributes')[4]['formatOptions']);

        // Date attribute
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('date', $attribute['key']);
        $this->assertEquals('string', $attribute['type']);
        $this->assertEquals(null, $attribute['default']);

        $database->updateAttribute($this->getFlowersCollection(), 'date', type: ColumnType::Datetime, size: 0, filters: ['datetime']);
        $collection = $database->getCollection($this->getFlowersCollection());
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('datetime', $attribute['type']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

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

        $database->createCollection('rename_test');

        $this->assertEquals(true, $database->createAttribute('rename_test', new Attribute(key: 'rename_me', type: ColumnType::String, size: 128, required: true)));

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
        $database->createIndex('rename_test', new Index(key: 'renameIndexes', type: IndexType::Key, attributes: ['rename_me'], lengths: [], orders: [OrderDirection::Desc->value, OrderDirection::Desc->value]));

        $database->updateAttribute(
            collection: 'rename_test',
            id: 'rename_me',
            newKey: 'renamed',
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
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['key']);
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals('renamed', $collection->getAttribute('indexes')[0]['attributes'][0]);

        $supportsIdenticalIndexes = $database->getAdapter()->supports(Capability::IdenticalIndexes);

        try {
            // Check empty newKey doesn't cause issues
            $database->updateAttribute(
                collection: 'rename_test',
                id: 'renamed',
                type: ColumnType::String,
            );

            if (! $supportsIdenticalIndexes) {
                $this->fail('Expected exception when getSupportForIdenticalIndexes=false but none was thrown');
            }
        } catch (Throwable $e) {
            if (! $supportsIdenticalIndexes) {
                $this->assertTrue(true, 'Exception thrown as expected when getSupportForIdenticalIndexes=false');

                return; // Exit early if exception was expected
            } else {
                $this->fail('Unexpected exception when getSupportForIdenticalIndexes=true: '.$e->getMessage());
            }
        }

        $collection = $database->getCollection('rename_test');

        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['key']);
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals('renamed', $collection->getAttribute('indexes')[0]['attributes'][0]);

        $doc = $database->getDocument('rename_test', $doc->getId());

        $this->assertEquals('string2', $doc->getAttribute('renamed'));
        $this->assertArrayNotHasKey('rename_me', $doc->getAttributes());

        // Check the metadata was correctly updated
        $attribute = $collection->getAttribute('attributes')[0];
        $this->assertEquals('renamed', $attribute['key']);
        $this->assertEquals('renamed', $attribute['$id']);

        // Check the indexes were updated
        $index = $collection->getAttribute('indexes')[0];
        $this->assertEquals('renamed', $index->getAttribute('attributes')[0]);
        $this->assertEquals(1, count($collection->getAttribute('indexes')));

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
            id: 'renamed',
            newKey: 'renamed-test',
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
        $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'name', type: ColumnType::String, size: 128, required: true));
        $database->createAttribute($collection, new Attribute(key: 'hex', type: ColumnType::String, size: 128, required: true));
        $database->createIndex($collection, new Index(key: 'index1', type: IndexType::Key, attributes: ['name'], lengths: [128], orders: [OrderDirection::Asc->value]));
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

        if ($database->getAdapter()->getDocumentSizeLimit() === 0) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $attributes = [];

        $attributes[] = new Document([
            '$id' => ID::custom('varchar_16000'),
            'type' => ColumnType::String->value,
            'size' => 16000,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $attributes[] = new Document([
            '$id' => ID::custom('varchar_200'),
            'type' => ColumnType::String->value,
            'size' => 200,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        try {
            $database->createCollection('attributes_row_size', $attributes);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Document size limit of 65535 exceeded. Cannot create collection.', $e->getMessage());
        }

        /**
         * Remove last attribute
         */
        array_pop($attributes);

        $collection = $database->createCollection('attributes_row_size', $attributes);

        $attribute = new Document([
            '$id' => ID::custom('breaking'),
            'type' => ColumnType::String->value,
            'size' => 200,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        try {
            $database->checkAttribute($collection, $attribute);
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertStringContainsString('Row width limit reached. Cannot create new attribute.', $e->getMessage());
            $this->assertStringContainsString('bytes but the maximum is 65535 bytes', $e->getMessage());
            $this->assertStringContainsString('Reduce the size of existing attributes or remove some attributes to free up space.', $e->getMessage());
        }

        try {
            $database->createAttribute($collection->getId(), new Attribute(key: 'breaking', type: ColumnType::String, size: 200, required: true));
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

        $database->createCollection('resize_test');

        $this->assertEquals(true, $database->createAttribute('resize_test', new Attribute(key: 'resize_me', type: ColumnType::String, size: 128, required: true)));
        $document = $database->createDocument('resize_test', new Document([
            '$id' => ID::unique(),
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
            $database->updateAttribute('resize_test', 'resize_me', ColumnType::String->value, 128, true);
            $this->fail('Succeeded updating attribute size to smaller size with data that is too big');
        } catch (TruncateException $e) {
        }

        // Test going down in size when data isn't too big.
        $database->updateDocument('resize_test', $document->getId(), $document->setAttribute('resize_me', $this->createRandomString(128)));
        $database->updateAttribute('resize_test', 'resize_me', ColumnType::String->value, 128, true);

        // VARCHAR -> VARCHAR Truncation Test
        $database->updateAttribute('resize_test', 'resize_me', ColumnType::String->value, 1000, true);
        $database->updateDocument('resize_test', $document->getId(), $document->setAttribute('resize_me', $this->createRandomString(1000)));

        try {
            $database->updateAttribute('resize_test', 'resize_me', ColumnType::String->value, 128, true);
            $this->fail('Succeeded updating attribute size to smaller size with data that is too big');
        } catch (TruncateException $e) {
        }

        if ($database->getAdapter()->getMaxIndexLength() > 0) {
            $length = intval($database->getAdapter()->getMaxIndexLength() / 2);

            $this->assertEquals(true, $database->createAttribute('resize_test', new Attribute(key: 'attr1', type: ColumnType::String, size: $length, required: true)));
            $this->assertEquals(true, $database->createAttribute('resize_test', new Attribute(key: 'attr2', type: ColumnType::String, size: $length, required: true)));

            /**
             * No index length provided, we are able to validate
             */
            $database->createIndex('resize_test', new Index(key: 'index1', type: IndexType::Key, attributes: ['attr1', 'attr2']));

            try {
                $database->updateAttribute('resize_test', 'attr1', ColumnType::String->value, 5000);
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
            }

            $database->deleteIndex('resize_test', 'index1');

            /**
             * Index lengths are provided, We are able to validate
             * Index $length === attr1, $length === attr2, so $length is removed, so we are able to validate
             */
            $database->createIndex('resize_test', new Index(key: 'index1', type: IndexType::Key, attributes: ['attr1', 'attr2'], lengths: [$length, $length]));

            $collection = $database->getCollection('resize_test');
            $indexes = $collection->getAttribute('indexes', []);
            $this->assertEquals(null, $indexes[0]['lengths'][0]);
            $this->assertEquals(null, $indexes[0]['lengths'][1]);

            try {
                $database->updateAttribute('resize_test', 'attr1', ColumnType::String->value, 5000);
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
            }

            $database->deleteIndex('resize_test', 'index1');

            /**
             * Index lengths are provided
             * We are able to increase size because index length remains 50
             */
            $database->createIndex('resize_test', new Index(key: 'index1', type: IndexType::Key, attributes: ['attr1', 'attr2'], lengths: [50, 50]));

            $collection = $database->getCollection('resize_test');
            $indexes = $collection->getAttribute('indexes', []);
            $this->assertEquals(50, $indexes[0]['lengths'][0]);
            $this->assertEquals(50, $indexes[0]['lengths'][1]);

            $database->updateAttribute('resize_test', 'attr1', ColumnType::String->value, 5000);
        }
    }

    public function testEncryptAttributes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        // Add custom encrypt filter
        $database->addFilter(
            'encrypt',
            function (mixed $value) {
                return json_encode([
                    'data' => base64_encode($value),
                    'method' => 'base64',
                    'version' => 'v1',
                ]);
            },
            function (mixed $value) {
                if (is_null($value)) {
                    return;
                }
                $value = json_decode($value, true);

                return base64_decode($value['data']);
            }
        );

        $col = $database->createCollection(__FUNCTION__);
        $this->assertNotNull($col->getId());

        $database->createAttribute($col->getId(), new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($col->getId(), new Attribute(key: 'encrypt', type: ColumnType::String, size: 128, required: true, filters: ['encrypt']));

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

    public function updateStringAttributeSize(int $size, Document $document): Document
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->updateAttribute('resize_test', 'resize_me', ColumnType::String->value, $size, true);

        $document = $document->setAttribute('resize_me', $this->createRandomString($size));

        $database->updateDocument('resize_test', $document->getId(), $document);
        $checkDoc = $database->getDocument('resize_test', $document->getId());

        $this->assertEquals($document->getAttribute('resize_me'), $checkDoc->getAttribute('resize_me'));
        $this->assertEquals($size, strlen($checkDoc->getAttribute('resize_me')));

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

        $database->createCollection($collection, permissions: [
            Permission::create(Role::any()),
        ]);

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'booleans', type: ColumnType::Boolean, size: 0, required: true, array: true)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'names', type: ColumnType::String, size: 255, required: false, array: true)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'cards', type: ColumnType::String, size: 5000, required: false, array: true)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'numbers', type: ColumnType::Integer, size: 0, required: false, array: true)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'age', type: ColumnType::Integer, size: 0, required: false, signed: false)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'tv_show', type: ColumnType::String, size: $database->getAdapter()->getMaxIndexLength() - 68, required: false, signed: false)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'short', type: ColumnType::String, size: 5, required: false, signed: false, array: true)));

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'pref', type: ColumnType::String, size: 16384, required: false, signed: false, filters: ['json'])));

        try {
            $database->createDocument($collection, new Document([]));
            $this->fail('Failed to throw exception');
        } catch (Throwable $e) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                $this->assertEquals('Invalid document structure: Missing required attribute "booleans"', $e->getMessage());
            }
        }

        $database->updateAttribute($collection, 'booleans', required: false);

        $doc = $database->getCollection($collection);
        $attribute = $doc->getAttribute('attributes')[0];
        $this->assertEquals('boolean', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(true, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);

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

        $this->assertEquals(false, $document->getAttribute('booleans')[0]);
        $this->assertEquals('Antony', $document->getAttribute('names')[1]);
        $this->assertEquals(100, $document->getAttribute('numbers')[1]);

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            /**
             * Functional index dependency cannot be dropped or rename
             */
            $database->createIndex($collection, new Index(key: 'idx_cards', type: IndexType::Key, attributes: ['cards'], lengths: [100]));
        }

        if ($database->getAdapter()->supports(Capability::CastIndexArray)) {
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
                $database->updateAttribute($collection, id: 'cards', newKey: 'cards_new');
                $this->fail('Failed to throw exception');
            } catch (Throwable $e) {
                $this->assertInstanceOf(DependencyException::class, $e);
                $this->assertEquals("Attribute can't be deleted or renamed because it is used in an index", $e->getMessage());
            }

        } else {
            $this->assertTrue($database->renameAttribute($collection, 'cards', 'cards_new'));
            $this->assertTrue($database->deleteAttribute($collection, 'cards_new'));
        }

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            try {
                $database->createIndex($collection, new Index(key: 'indx', type: IndexType::Fulltext, attributes: ['names']));
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $this->fail('Failed to throw exception');
                }
            } catch (Throwable $e) {
                if ($database->getAdapter()->supports(Capability::Fulltext)) {
                    $this->assertEquals('"Fulltext" index is forbidden on array attributes', $e->getMessage());
                } else {
                    $this->assertEquals('Fulltext index is not supported', $e->getMessage());
                }
            }

            try {
                $database->createIndex($collection, new Index(key: 'indx', type: IndexType::Key, attributes: ['numbers', 'names'], lengths: [100, 100]));
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

        $this->assertEquals(true, $database->createAttribute($collection, new Attribute(key: 'long_size', type: ColumnType::String, size: 2000, required: false, array: true)));

        if ($database->getAdapter()->supports(Capability::IndexArray)) {
            if ($database->getAdapter()->supports(Capability::DefinedAttributes) && $database->getAdapter()->getMaxIndexLength() > 0) {
                // If getMaxIndexLength() > 0 We clear length for array attributes
                $database->createIndex($collection, new Index(key: 'indx1', type: IndexType::Key, attributes: ['long_size'], lengths: [], orders: []));
                $database->deleteIndex($collection, 'indx1');
                $database->createIndex($collection, new Index(key: 'indx2', type: IndexType::Key, attributes: ['long_size'], lengths: [1000], orders: []));

                try {
                    $database->createIndex($collection, new Index(key: 'indx_numbers', type: IndexType::Key, attributes: ['tv_show', 'numbers'], lengths: [], orders: [])); // [700, 255]
                    $this->fail('Failed to throw exception');
                } catch (Throwable $e) {
                    $this->assertEquals('Index length is longer than the maximum: '.$database->getAdapter()->getMaxIndexLength(), $e->getMessage());
                }
            }

            try {
                if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
                    $database->createIndex($collection, new Index(key: 'indx4', type: IndexType::Key, attributes: ['age', 'names'], lengths: [10, 255], orders: []));
                    $this->fail('Failed to throw exception');
                }
            } catch (Throwable $e) {
                $this->assertEquals('Cannot set a length on "integer" attributes', $e->getMessage());
            }

            $this->assertTrue($database->createIndex($collection, new Index(key: 'indx6', type: IndexType::Key, attributes: ['age', 'names'], lengths: [null, 999], orders: [])));
            $this->assertTrue($database->createIndex($collection, new Index(key: 'indx7', type: IndexType::Key, attributes: ['age', 'booleans'], lengths: [0, 999], orders: [])));
        }

        if ($this->getDatabase()->getAdapter()->supports(Capability::QueryContains)) {
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
                    Query::contains('age', [10]),
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
                Query::contains('tv_show', ['love']),
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('names', ['Jake', 'Joe']),
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('numbers', [-1, 0, 999]),
            ]);
            $this->assertCount(1, $documents);

            $documents = $database->find($collection, [
                Query::contains('booleans', [false, true]),
            ]);
            $this->assertCount(1, $documents);

            // Regular like query on primitive json string data
            $documents = $database->find($collection, [
                Query::contains('pref', ['Joe']),
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
    }

    public function testCreateDatetime(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('datetime');
        if ($database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->assertEquals(true, $database->createAttribute('datetime', new Attribute(key: 'date', type: ColumnType::Datetime, size: 0, required: true, default: null, signed: true, array: false, format: null, formatOptions: [], filters: ['datetime'])));
            $this->assertEquals(true, $database->createAttribute('datetime', new Attribute(key: 'date2', type: ColumnType::Datetime, size: 0, required: false, default: null, signed: true, array: false, format: null, formatOptions: [], filters: ['datetime'])));
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
            '$id' => ID::custom('id1234'),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'date' => DateTime::now(),
        ]));

        $this->assertEquals(29, strlen($doc->getCreatedAt()));
        $this->assertEquals(29, strlen($doc->getUpdatedAt()));
        $this->assertEquals('+00:00', substr($doc->getCreatedAt(), -6));
        $this->assertEquals('+00:00', substr($doc->getUpdatedAt(), -6));
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getCreatedAt());
        $this->assertGreaterThan('2020-08-16T19:30:08.363+00:00', $doc->getUpdatedAt());

        $document = $database->getDocument('datetime', 'id1234');

        $min = $database->getAdapter()->getMinDateTime();
        $max = $database->getAdapter()->getMaxDateTime();
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
            '2024-12-2509:00:21.891119',
            'Tue Dec 31 2024',
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

        $database->createCollection('datetime_auto_filter');

        $this->expectException(Exception::class);
        $database->createAttribute('datetime_auto', new Attribute(key: 'date_auto', type: ColumnType::Datetime, size: 0, required: false, filters: ['json']));
        $collection = $database->getCollection('datetime_auto_filter');
        $attribute = $collection->getAttribute('attributes')[0];
        $this->assertEquals([ColumnType::Datetime->value, 'json'], $attribute['filters']);
        $database->updateAttribute('datetime_auto', 'date_auto', ColumnType::Datetime->value, 0, false, filters: []);
        $collection = $database->getCollection('datetime_auto_filter');
        $attribute = $collection->getAttribute('attributes')[0];
        $this->assertEquals([ColumnType::Datetime->value, 'json'], $attribute['filters']);
        $database->deleteCollection('datetime_auto_filter');
    }

    public function testCreateAttributesSuccessMultiple(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::BatchCreateAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(__FUNCTION__);

        $attributes = [new Attribute(key: 'a', type: ColumnType::String, size: 10, required: false), new Attribute(key: 'b', type: ColumnType::Integer, size: 0, required: false)];

        $result = $database->createAttributes(__FUNCTION__, $attributes);
        $this->assertTrue($result);

        $collection = $database->getCollection(__FUNCTION__);
        $attrs = $collection->getAttribute('attributes');
        $this->assertCount(2, $attrs);
        $this->assertEquals('a', $attrs[0]['$id']);
        $this->assertEquals('b', $attrs[1]['$id']);

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

        if (! $database->getAdapter()->supports(Capability::BatchCreateAttributes)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(__FUNCTION__);

        $attributes = [new Attribute(key: 'a', type: ColumnType::String, size: 10, required: false), new Attribute(key: 'b', type: ColumnType::Integer, size: 0, required: false)];

        $result = $database->createAttributes(__FUNCTION__, $attributes);
        $this->assertTrue($result);

        $collection = $database->getCollection(__FUNCTION__);
        $attrs = $collection->getAttribute('attributes');
        $this->assertCount(2, $attrs);
        $this->assertEquals('a', $attrs[0]['$id']);
        $this->assertEquals('b', $attrs[1]['$id']);

        $database->deleteAttribute(__FUNCTION__, 'a');

        $collection = $database->getCollection(__FUNCTION__);
        $attrs = $collection->getAttribute('attributes');
        $this->assertCount(1, $attrs);
        $this->assertEquals('b', $attrs[0]['$id']);
    }

    public function testStringTypeAttributes(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        $database->createCollection('stringTypes');

        // Create attributes with different string types
        $this->assertEquals(true, $database->createAttribute('stringTypes', new Attribute(key: 'varchar_field', type: ColumnType::Varchar, size: 255, required: false, default: 'default varchar')));
        $this->assertEquals(true, $database->createAttribute('stringTypes', new Attribute(key: 'text_field', type: ColumnType::Text, size: 65535, required: false)));
        $this->assertEquals(true, $database->createAttribute('stringTypes', new Attribute(key: 'mediumtext_field', type: ColumnType::MediumText, size: 16777215, required: false)));
        $this->assertEquals(true, $database->createAttribute('stringTypes', new Attribute(key: 'longtext_field', type: ColumnType::LongText, size: 4294967295, required: false)));

        // Test with array types
        $this->assertEquals(true, $database->createAttribute('stringTypes', new Attribute(key: 'varchar_array', type: ColumnType::Varchar, size: 128, required: false, default: null, signed: true, array: true)));
        $this->assertEquals(true, $database->createAttribute('stringTypes', new Attribute(key: 'text_array', type: ColumnType::Text, size: 65535, required: false, default: null, signed: true, array: true)));

        $collection = $database->getCollection('stringTypes');
        $this->assertCount(6, $collection->getAttribute('attributes'));

        // Test VARCHAR with valid data
        $doc1 = $database->createDocument('stringTypes', new Document([
            '$id' => ID::custom('doc1'),
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
            '$id' => ID::custom('doc2'),
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
            '$id' => ID::custom('doc3'),
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
                    '$id' => ID::custom('doc4'),
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
                    '$id' => ID::custom('doc5'),
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
        $this->assertEquals(true, $database->createIndex('stringTypes', new Index(key: 'varchar_index', type: IndexType::Key, attributes: ['varchar_field'])));

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
}
