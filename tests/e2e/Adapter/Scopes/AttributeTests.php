<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Structure;
use Utopia\Validator\Range;

trait AttributeTests
{
    /**
     * Using phpunit dataProviders to check that all these combinations of types/defaults throw exceptions
     * https://phpunit.de/manual/3.7/en/writing-tests-for-phpunit.html#writing-tests-for-phpunit.data-providers
     *
     * @return array<array<bool|float|int|string>>
     */
    public function invalidDefaultValues(): array
    {
        return [
            [Database::VAR_STRING, 1],
            [Database::VAR_STRING, 1.5],
            [Database::VAR_STRING, false],
            [Database::VAR_INTEGER, "one"],
            [Database::VAR_INTEGER, 1.5],
            [Database::VAR_INTEGER, true],
            [Database::VAR_FLOAT, 1],
            [Database::VAR_FLOAT, "one"],
            [Database::VAR_FLOAT, false],
            [Database::VAR_BOOLEAN, 0],
            [Database::VAR_BOOLEAN, "false"],
            [Database::VAR_BOOLEAN, 0.5],
        ];
    }

    public function testCreateDeleteAttribute(): void
    {
        static::getDatabase()->createCollection('attributes');

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string2', Database::VAR_STRING, 16382 + 1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string3', Database::VAR_STRING, 65535 + 1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string4', Database::VAR_STRING, 16777215 + 1, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer', Database::VAR_INTEGER, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'bigint', Database::VAR_INTEGER, 8, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float', Database::VAR_FLOAT, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean', Database::VAR_BOOLEAN, 0, true));

        $this->assertEquals(true, static::getDatabase()->createIndex('attributes', 'string1_index', Database::INDEX_KEY, ['string1']));
        $this->assertEquals(true, static::getDatabase()->createIndex('attributes', 'string2_index', Database::INDEX_KEY, ['string2'], [255]));
        $this->assertEquals(true, static::getDatabase()->createIndex('attributes', 'multi_index', Database::INDEX_KEY, ['string1', 'string2', 'string3'], [128, 128, 128]));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(8, $collection->getAttribute('attributes'));
        $this->assertCount(3, $collection->getAttribute('indexes'));

        // Array
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_list', Database::VAR_STRING, 128, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_list', Database::VAR_INTEGER, 0, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_list', Database::VAR_FLOAT, 0, true, null, true, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_list', Database::VAR_BOOLEAN, 0, true, null, true, true));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(12, $collection->getAttribute('attributes'));

        // Default values
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string_default', Database::VAR_STRING, 256, false, 'test'));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'integer_default', Database::VAR_INTEGER, 0, false, 1));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'float_default', Database::VAR_FLOAT, 0, false, 1.5));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'boolean_default', Database::VAR_BOOLEAN, 0, false, false));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'datetime_default', Database::VAR_DATETIME, 0, false, '2000-06-12T14:12:55.000+00:00', true, false, null, [], ['datetime']));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(17, $collection->getAttribute('attributes'));

        // Delete
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string1'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string2'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string3'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string4'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'bigint'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(9, $collection->getAttribute('attributes'));
        $this->assertCount(0, $collection->getAttribute('indexes'));

        // Delete Array
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_list'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_list'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(5, $collection->getAttribute('attributes'));

        // Delete default
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'integer_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'float_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'boolean_default'));
        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'datetime_default'));

        $collection = static::getDatabase()->getCollection('attributes');
        $this->assertCount(0, $collection->getAttribute('attributes'));

        // Test for custom chars in ID
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as_5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as5dasdasdas_', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '.as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '-as5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as-5dasdasdas', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'as5dasdasdas-', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'socialAccountForYoutubeSubscribersss', Database::VAR_BOOLEAN, 0, true));
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', '5f058a89258075f058a89258075f058t9214', Database::VAR_BOOLEAN, 0, true));

        // Test non-shared tables duplicates throw duplicate
        static::getDatabase()->createAttribute('attributes', 'duplicate', Database::VAR_STRING, 128, true);
        try {
            static::getDatabase()->createAttribute('attributes', 'duplicate', Database::VAR_STRING, 128, true);
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Test delete attribute when column does not exist
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'string1', Database::VAR_STRING, 128, true));
        sleep(1);

        $this->assertEquals(true, static::deleteColumn('attributes', 'string1'));

        $collection = static::getDatabase()->getCollection('attributes');
        $attributes = $collection->getAttribute('attributes');
        $attribute = end($attributes);
        $this->assertEquals('string1', $attribute->getId());

        $this->assertEquals(true, static::getDatabase()->deleteAttribute('attributes', 'string1'));

        $collection = static::getDatabase()->getCollection('attributes');
        $attributes = $collection->getAttribute('attributes');
        $attribute = end($attributes);
        $this->assertNotEquals('string1', $attribute->getId());

        $collection = static::getDatabase()->getCollection('attributes');
    }
    /**
     * @depends      testCreateDeleteAttribute
     * @dataProvider invalidDefaultValues
     */
    public function testInvalidDefaultValues(string $type, mixed $default): void
    {
        $this->expectException(\Exception::class);
        $this->assertEquals(false, static::getDatabase()->createAttribute('attributes', 'bad_default', $type, 256, true, $default));
    }
    /**
    * @depends testInvalidDefaultValues
    */
    public function testAttributeCaseInsensitivity(): void
    {

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'caseSensitive', Database::VAR_STRING, 128, true));
        $this->expectException(DuplicateException::class);
        $this->assertEquals(true, static::getDatabase()->createAttribute('attributes', 'CaseSensitive', Database::VAR_STRING, 128, true));
    }

    public function testAttributeKeyWithSymbols(): void
    {
        static::getDatabase()->createCollection('attributesWithKeys');

        $this->assertEquals(true, static::getDatabase()->createAttribute('attributesWithKeys', 'key_with.sym$bols', Database::VAR_STRING, 128, true));

        $document = static::getDatabase()->createDocument('attributesWithKeys', new Document([
            'key_with.sym$bols' => 'value',
            '$permissions' => [
                Permission::read(Role::any()),
            ]
        ]));

        $this->assertEquals('value', $document->getAttribute('key_with.sym$bols'));

        $document = static::getDatabase()->getDocument('attributesWithKeys', $document->getId());

        $this->assertEquals('value', $document->getAttribute('key_with.sym$bols'));
    }

    public function testAttributeNamesWithDots(): void
    {
        static::getDatabase()->createCollection('dots.parent');

        $this->assertTrue(static::getDatabase()->createAttribute(
            collection: 'dots.parent',
            id: 'dots.name',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        $document = static::getDatabase()->find('dots.parent', [
            Query::select(['dots.name']),
        ]);
        $this->assertEmpty($document);

        static::getDatabase()->createCollection('dots');

        $this->assertTrue(static::getDatabase()->createAttribute(
            collection: 'dots',
            id: 'name',
            type: Database::VAR_STRING,
            size: 255,
            required: false
        ));

        static::getDatabase()->createRelationship(
            collection: 'dots.parent',
            relatedCollection: 'dots',
            type: Database::RELATION_ONE_TO_ONE
        );

        static::getDatabase()->createDocument('dots.parent', new Document([
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
            ]
        ]));

        $documents = static::getDatabase()->find('dots.parent', [
            Query::select(['*']),
        ]);

        $this->assertEquals('Bill clinton', $documents[0]['dots.name']);
    }


    public function testUpdateAttributeDefault(): void
    {
        $database = static::getDatabase();

        $flowers = $database->createCollection('flowers');
        $database->createAttribute('flowers', 'name', Database::VAR_STRING, 128, true);
        $database->createAttribute('flowers', 'inStock', Database::VAR_INTEGER, 0, false);
        $database->createAttribute('flowers', 'date', Database::VAR_STRING, 128, false);

        $database->createDocument('flowers', new Document([
            '$id' => 'flowerWithDate',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Violet',
            'inStock' => 51,
            'date' => '2000-06-12 14:12:55.000'
        ]));

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily'
        ]));

        $this->assertNull($doc->getAttribute('inStock'));

        $database->updateAttributeDefault('flowers', 'inStock', 100);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Iris'
        ]));

        $this->assertIsNumeric($doc->getAttribute('inStock'));
        $this->assertEquals(100, $doc->getAttribute('inStock'));

        $database->updateAttributeDefault('flowers', 'inStock', null);
    }


    public function testRenameAttribute(): void
    {
        $database = static::getDatabase();

        $colors = $database->createCollection('colors');
        $database->createAttribute('colors', 'name', Database::VAR_STRING, 128, true);
        $database->createAttribute('colors', 'hex', Database::VAR_STRING, 128, true);

        $database->createIndex('colors', 'index1', Database::INDEX_KEY, ['name'], [128], [Database::ORDER_ASC]);

        $database->createDocument('colors', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'black',
            'hex' => '#000000'
        ]));

        $attribute = $database->renameAttribute('colors', 'name', 'verbose');

        $this->assertTrue($attribute);

        $colors = $database->getCollection('colors');
        $this->assertEquals('hex', $colors->getAttribute('attributes')[1]['$id']);
        $this->assertEquals('verbose', $colors->getAttribute('attributes')[0]['$id']);
        $this->assertCount(2, $colors->getAttribute('attributes'));

        // Attribute in index is renamed automatically on adapter-level. What we need to check is if metadata is properly updated
        $this->assertEquals('verbose', $colors->getAttribute('indexes')[0]->getAttribute("attributes")[0]);
        $this->assertCount(1, $colors->getAttribute('indexes'));

        // Document should be there if adapter migrated properly
        $document = $database->findOne('colors');
        $this->assertFalse($document->isEmpty());
        $this->assertEquals('black', $document->getAttribute('verbose'));
        $this->assertEquals('#000000', $document->getAttribute('hex'));
        $this->assertEquals(null, $document->getAttribute('name'));
    }


    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeRequired(): void
    {
        $database = static::getDatabase();

        $database->updateAttributeRequired('flowers', 'inStock', true);

        $this->expectExceptionMessage('Invalid document structure: Missing required attribute "inStock"');

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With Missing Stocks'
        ]));
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeFilter(): void
    {
        $database = static::getDatabase();

        $database->createAttribute('flowers', 'cartModel', Database::VAR_STRING, 2000, false);

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily With CartData',
            'inStock' => 50,
            'cartModel' => '{"color":"string","size":"number"}'
        ]));

        $this->assertIsString($doc->getAttribute('cartModel'));
        $this->assertEquals('{"color":"string","size":"number"}', $doc->getAttribute('cartModel'));

        $database->updateAttributeFilters('flowers', 'cartModel', ['json']);

        $doc = $database->getDocument('flowers', $doc->getId());
        $this->assertIsArray($doc->getAttribute('cartModel'));
        $this->assertCount(2, $doc->getAttribute('cartModel'));
        $this->assertEquals('string', $doc->getAttribute('cartModel')['color']);
        $this->assertEquals('number', $doc->getAttribute('cartModel')['size']);
    }

    /**
     * @depends testUpdateAttributeDefault
     */
    public function testUpdateAttributeFormat(): void
    {
        $database = static::getDatabase();

        $database->createAttribute('flowers', 'price', Database::VAR_INTEGER, 0, false);

        $doc = $database->createDocument('flowers', new Document([
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
            'price' => 500
        ]));

        $this->assertIsNumeric($doc->getAttribute('price'));
        $this->assertEquals(500, $doc->getAttribute('price'));

        Structure::addFormat('priceRange', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];

            return new Range($min, $max);
        }, Database::VAR_INTEGER);

        $database->updateAttributeFormat('flowers', 'price', 'priceRange');
        $database->updateAttributeFormatOptions('flowers', 'price', ['min' => 1, 'max' => 10000]);

        $this->expectExceptionMessage('Invalid document structure: Attribute "price" has invalid format. Value must be a valid range between 1 and 10,000');

        $doc = $database->createDocument('flowers', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Lily Overpriced',
            'inStock' => 50,
            'cartModel' => '{}',
            'price' => 15000
        ]));
    }

    /**
     * @depends testUpdateAttributeDefault
     * @depends testUpdateAttributeFormat
     */
    public function testUpdateAttributeStructure(): void
    {
        // TODO: When this becomes relevant, add many more tests (from all types to all types, chaging size up&down, switchign between array/non-array...

        Structure::addFormat('priceRangeNew', function ($attribute) {
            $min = $attribute['formatOptions']['min'];
            $max = $attribute['formatOptions']['max'];
            return new Range($min, $max);
        }, Database::VAR_INTEGER);

        $database = static::getDatabase();

        // price attribute
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', default: 100);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRange', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', format: 'priceRangeNew');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('priceRangeNew', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', format: '');
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 10000], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', formatOptions: ['min' => 1, 'max' => 999]);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals(['min' => 1, 'max' => 999], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', formatOptions: []);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', signed: false);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(100, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', required: true);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[4];
        $this->assertEquals('integer', $attribute['type']);
        $this->assertEquals(false, $attribute['signed']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals(true, $attribute['required']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $database->updateAttribute('flowers', 'price', type: Database::VAR_STRING, size: Database::LENGTH_KEY, format: '');
        $collection = $database->getCollection('flowers');
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

        $database->updateAttribute('flowers', 'date', type: Database::VAR_DATETIME, size: 0, filters: ['datetime']);
        $collection = $database->getCollection('flowers');
        $attribute = $collection->getAttribute('attributes')[2];
        $this->assertEquals('datetime', $attribute['type']);
        $this->assertEquals(0, $attribute['size']);
        $this->assertEquals(null, $attribute['default']);
        $this->assertEquals(false, $attribute['required']);
        $this->assertEquals(true, $attribute['signed']);
        $this->assertEquals(false, $attribute['array']);
        $this->assertEquals('', $attribute['format']);
        $this->assertEquals([], $attribute['formatOptions']);

        $doc = $database->getDocument('flowers', 'LiliPriced');
        $this->assertIsString($doc->getAttribute('price'));
        $this->assertEquals('500', $doc->getAttribute('price'));

        $doc = $database->getDocument('flowers', 'flowerWithDate');
        $this->assertEquals('2000-06-12T14:12:55.000+00:00', $doc->getAttribute('date'));
    }

    public function testUpdateAttributeRename(): void
    {
        static::getDatabase()->createCollection('rename_test');

        $this->assertEquals(true, static::getDatabase()->createAttribute('rename_test', 'rename_me', Database::VAR_STRING, 128, true));

        $doc = static::getDatabase()->createDocument('rename_test', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'rename_me' => 'string'
        ]));

        $this->assertEquals('string', $doc->getAttribute('rename_me'));

        // Create an index to check later
        static::getDatabase()->createIndex('rename_test', 'renameIndexes', Database::INDEX_KEY, ['rename_me'], [], [Database::ORDER_DESC, Database::ORDER_DESC]);

        static::getDatabase()->updateAttribute(
            collection: 'rename_test',
            id: 'rename_me',
            newKey: 'renamed',
        );

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());

        // Check the attribute was correctly renamed
        $this->assertEquals('string', $doc->getAttribute('renamed'));
        $this->assertArrayNotHasKey('rename_me', $doc);

        // Check we can update the document with the new key
        $doc->setAttribute('renamed', 'string2');
        static::getDatabase()->updateDocument('rename_test', $doc->getId(), $doc);

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());
        $this->assertEquals('string2', $doc->getAttribute('renamed'));

        // Check collection
        $collection = static::getDatabase()->getCollection('rename_test');
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['key']);
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals('renamed', $collection->getAttribute('indexes')[0]['attributes'][0]);

        // Check empty newKey doesn't cause issues
        static::getDatabase()->updateAttribute(
            collection: 'rename_test',
            id: 'renamed',
            type: Database::VAR_STRING,
        );

        $collection = static::getDatabase()->getCollection('rename_test');

        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['key']);
        $this->assertEquals('renamed', $collection->getAttribute('attributes')[0]['$id']);
        $this->assertEquals('renamed', $collection->getAttribute('indexes')[0]['attributes'][0]);

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());

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
        $doc = static::getDatabase()->createDocument('rename_test', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'renamed' => 'string'
        ]));

        $this->assertEquals('string', $doc->getAttribute('renamed'));

        // Make sure we can't create a new attribute with the old key
        try {
            $doc = static::getDatabase()->createDocument('rename_test', new Document([
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'rename_me' => 'string'
            ]));
            $this->fail('Succeeded creating a document with old key after renaming the attribute');
        } catch (\Exception $e) {
            $this->assertInstanceOf(StructureException::class, $e);
        }

        // Check new key filtering
        static::getDatabase()->updateAttribute(
            collection: 'rename_test',
            id: 'renamed',
            newKey: 'renamed-test',
        );

        $doc = static::getDatabase()->getDocument('rename_test', $doc->getId());

        $this->assertEquals('string', $doc->getAttribute('renamed-test'));
        $this->assertArrayNotHasKey('renamed', $doc->getAttributes());
    }


    /**
     * @depends testRenameAttribute
     * @expectedException Exception
     */
    public function textRenameAttributeMissing(): void
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Attribute not found');
        $database->renameAttribute('colors', 'name2', 'name3');
    }

    /**
    * @depends testRenameAttribute
    * @expectedException Exception
    */
    public function testRenameAttributeExisting(): void
    {
        $database = static::getDatabase();
        $this->expectExceptionMessage('Attribute name already used');
        $database->renameAttribute('colors', 'verbose', 'hex');
    }

    public function testWidthLimit(): void
    {
        if (static::getDatabase()->getAdapter()->getDocumentSizeLimit() === 0) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collection = static::getDatabase()->createCollection('width_limit');

        $init = static::getDatabase()->getAdapter()->getAttributeWidth($collection);
        $this->assertEquals(1067, $init);

        $attribute = new Document([
            '$id' => ID::custom('varchar_100'),
            'type' => Database::VAR_STRING,
            'size' => 100,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);
        $res = static::getDatabase()->getAdapter()->getAttributeWidth($collection->setAttribute('attributes', [$attribute]));
        $this->assertEquals(401, $res - $init); // 100 * 4 + 1 (length)

        $attribute = new Document([
            '$id' => ID::custom('json'),
            'type' => Database::VAR_STRING,
            'size' => 100,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ]);
        $res = static::getDatabase()->getAdapter()->getAttributeWidth($collection->setAttribute('attributes', [$attribute]));
        $this->assertEquals(20, $res - $init); // Pointer of Json / Longtext (mariaDB)

        $attribute = new Document([
            '$id' => ID::custom('text'),
            'type' => Database::VAR_STRING,
            'size' => 20000,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);
        $res = static::getDatabase()->getAdapter()->getAttributeWidth($collection->setAttribute('attributes', [$attribute]));
        $this->assertEquals(20, $res - $init);

        $attribute = new Document([
            '$id' => ID::custom('bigint'),
            'type' => Database::VAR_INTEGER,
            'size' => 8,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);
        $res = static::getDatabase()->getAdapter()->getAttributeWidth($collection->setAttribute('attributes', [$attribute]));
        $this->assertEquals(8, $res - $init);

        $attribute = new Document([
            '$id' => ID::custom('date'),
            'type' => Database::VAR_DATETIME,
            'size' => 8,
            'required' => false,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);
        $res = static::getDatabase()->getAdapter()->getAttributeWidth($collection->setAttribute('attributes', [$attribute]));
        $this->assertEquals(7, $res - $init);
    }

    public function testExceptionAttributeLimit(): void
    {
        if (static::getDatabase()->getAdapter()->getLimitForAttributes() === 0) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $limit = static::getDatabase()->getAdapter()->getLimitForAttributes() - static::getDatabase()->getAdapter()->getCountOfDefaultAttributes();

        $attributes = [];

        for ($i = 0; $i <= $limit; $i++) {
            $attributes[] = new Document([
                '$id' => ID::custom("attr_{$i}"),
                'type' => Database::VAR_INTEGER,
                'size' => 0,
                'required' => false,
                'default' => null,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]);
        }

        try {
            static::getDatabase()->createCollection('attributes_limit', $attributes);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Attribute limit of 1017 exceeded. Cannot create collection.', $e->getMessage());
        }

        /**
         * Remove last attribute
         */

        array_pop($attributes);

        $collection = static::getDatabase()->createCollection('attributes_limit', $attributes);

        $attribute = new Document([
            '$id' => ID::custom('breaking'),
            'type' => Database::VAR_STRING,
            'size' => 100,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        try {
            static::getDatabase()->checkAttribute($collection, $attribute);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Column limit reached. Cannot create new attribute.', $e->getMessage());
        }

        try {
            static::getDatabase()->createAttribute($collection->getId(), 'breaking', Database::VAR_STRING, 100, true);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Column limit reached. Cannot create new attribute.', $e->getMessage());
        }
    }

    public function testExceptionWidthLimit(): void
    {
        if (static::getDatabase()->getAdapter()->getDocumentSizeLimit() === 0) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $attributes = [];

        $attributes[] = new Document([
            '$id' => ID::custom('varchar_16000'),
            'type' => Database::VAR_STRING,
            'size' => 16000,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        $attributes[] = new Document([
            '$id' => ID::custom('varchar_200'),
            'type' => Database::VAR_STRING,
            'size' => 200,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        try {
            static::getDatabase()->createCollection("attributes_row_size", $attributes);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Document size limit of 65535 exceeded. Cannot create collection.', $e->getMessage());
        }

        /**
         * Remove last attribute
         */

        array_pop($attributes);

        $collection = static::getDatabase()->createCollection("attributes_row_size", $attributes);

        $attribute = new Document([
            '$id' => ID::custom('breaking'),
            'type' => Database::VAR_STRING,
            'size' => 200,
            'required' => true,
            'default' => null,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);

        try {
            static::getDatabase()->checkAttribute($collection, $attribute);
            $this->fail('Failed to throw exception');
        } catch (\Exception $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Row width limit reached. Cannot create new attribute.', $e->getMessage());
        }

        try {
            static::getDatabase()->createAttribute($collection->getId(), 'breaking', Database::VAR_STRING, 200, true);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(LimitException::class, $e);
            $this->assertEquals('Row width limit reached. Cannot create new attribute.', $e->getMessage());
        }
    }
}
