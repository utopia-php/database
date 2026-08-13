<?php

namespace Tests\Unit\Documents;

use DateTime;
use InvalidArgumentException;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Query\Schema\ColumnType;

class IncreaseDecreaseTest extends TestCase
{
    private bool $definedAttributes = true;

    private Adapter&Stub $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = self::createStub(Adapter::class);
        $this->adapter->method('getSharedTables')->willReturn(false);
        $this->adapter->method('getTenant')->willReturn(null);
        $this->adapter->method('getTenantPerDocument')->willReturn(false);
        $this->adapter->method('getNamespace')->willReturn('');
        $this->adapter->method('getIdAttributeType')->willReturn('string');
        $this->adapter->method('getMaxUIDLength')->willReturn(36);
        $this->adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $this->adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $this->adapter->method('getLimitForString')->willReturn(16777215);
        $this->adapter->method('getLimitForInt')->willReturn(2147483647);
        $this->adapter->method('getLimitForAttributes')->willReturn(0);
        $this->adapter->method('getLimitForIndexes')->willReturn(64);
        $this->adapter->method('getMaxIndexLength')->willReturn(768);
        $this->adapter->method('getMaxVarcharLength')->willReturn(16383);
        $this->adapter->method('getDocumentSizeLimit')->willReturn(0);
        $this->adapter->method('getCountOfAttributes')->willReturn(0);
        $this->adapter->method('getCountOfIndexes')->willReturn(0);
        $this->adapter->method('getAttributeWidth')->willReturn(0);
        $this->adapter->method('getInternalIndexesKeys')->willReturn([]);
        $this->adapter->method('filter')->willReturnArgument(0);
        $this->adapter->method('supports')->willReturnCallback(function (Capability $cap) {
            if ($cap === Capability::DefinedAttributes) {
                return $this->definedAttributes;
            }

            return in_array($cap, [
                Capability::Index,
                Capability::IndexArray,
                Capability::UniqueIndex,
                Capability::UnsignedBigInt,
            ]);
        });
        $this->adapter->method('castingBefore')->willReturnArgument(1);
        $this->adapter->method('castingAfter')->willReturnArgument(1);
        $this->adapter->method('startTransaction')->willReturn(true);
        $this->adapter->method('commitTransaction')->willReturn(true);
        $this->adapter->method('rollbackTransaction')->willReturn(true);
        $this->adapter->method('withTransaction')->willReturnCallback(function (callable $callback) {
            return $callback();
        });
        $this->adapter->method('increaseDocumentAttribute')->willReturn(true);

        $cache = new Cache(new None());
        $this->database = new Database($this->adapter, $cache);
        $this->database->getAuthorization()->addRole(Role::any()->toString());
    }

    private function setupCollectionWithDocument(
        string $collectionId,
        Document $existingDoc,
        array $attributes = [],
    ): void {
        $permissions = [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $collection = new Document([
            '$id' => $collectionId,
            '$collection' => Database::METADATA,
            '$permissions' => $permissions,
            'name' => $collectionId,
            'attributes' => $attributes,
            'indexes' => [],
            'documentSecurity' => true,
        ]);

        $this->adapter->method('getDocument')->willReturnCallback(
            function (Document $col, string $docId) use ($collectionId, $collection, $existingDoc) {
                if ($col->getId() === Database::METADATA && $docId === $collectionId) {
                    return $collection;
                }
                if ($col->getId() === Database::METADATA && $docId === Database::METADATA) {
                    return new Document(Database::COLLECTION);
                }
                if ($col->getId() === $collectionId && $docId === $existingDoc->getId()) {
                    return $existingDoc;
                }

                return new Document();
            }
        );
    }

    private function intAttribute(string $key): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => ColumnType::Integer->value,
            'size' => 0,
            'required' => false,
            'array' => false,
            'signed' => true,
            'filters' => [],
        ]);
    }

    private function floatAttribute(string $key): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => ColumnType::Double->value,
            'size' => 0,
            'required' => false,
            'array' => false,
            'signed' => true,
            'filters' => [],
        ]);
    }

    private function numericAttribute(string $key, ColumnType|string $type, bool $signed = true): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => $type instanceof ColumnType ? $type->value : $type,
            'size' => 0,
            'required' => false,
            'array' => false,
            'signed' => $signed,
            'filters' => [],
        ]);
    }

    public function testIncreaseDocumentAttribute(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter');
        $this->assertSame(6, $result->getAttribute('counter'));
    }

    public function testSchemalessIncreaseDefaultsOnlyMissingAttributeToZero(): void
    {
        $this->definedAttributes = false;
        $missing = new Document([
            '$id' => 'missing',
            '$collection' => 'testCol',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]);
        $this->setupCollectionWithDocument('testCol', $missing);

        $result = $this->database->increaseDocumentAttribute('testCol', 'missing', 'counter');

        $this->assertSame(1, $result->getAttribute('counter'));
    }

    public function testSchemalessIncreaseRejectsExplicitNull(): void
    {
        $this->definedAttributes = false;
        $nullable = new Document([
            '$id' => 'nullable',
            '$collection' => 'testCol',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => null,
        ]);
        $this->setupCollectionWithDocument('testCol', $nullable);

        $this->expectException(TypeException::class);
        $this->database->increaseDocumentAttribute('testCol', 'nullable', 'counter');
    }

    public function testIncreaseDocumentAttributeByCustomValue(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'score' => 10.0,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->floatAttribute('score')]);

        $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'score', 2.5);
        $this->assertSame(12.5, $result->getAttribute('score'));
    }

    public function testIncreaseAcceptsFloatBigIntegerBigSerialAndLegacyMetadata(): void
    {
        $types = [
            'float' => ColumnType::Float,
            'biginteger' => ColumnType::BigInteger,
            'bigserial' => ColumnType::BigSerial,
            'legacy' => 'bigint',
        ];
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'float' => 5,
            'biginteger' => 5,
            'bigserial' => 5,
            'legacy' => 5,
        ]);
        $attributes = [];
        foreach ($types as $key => $type) {
            $attributes[] = $this->numericAttribute($key, $type);
        }
        $this->setupCollectionWithDocument('testCol', $doc, $attributes);

        foreach (\array_keys($types) as $key) {
            $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', $key);

            $this->assertSame(6, $result->getAttribute($key), $key);
        }
    }

    public function testIncreaseRejectsBigIntegerOverflow(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => PHP_INT_MAX,
        ]);
        $this->setupCollectionWithDocument('testCol', $doc, [
            $this->numericAttribute('value', ColumnType::BigInteger),
        ]);

        $this->expectException(LimitException::class);
        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'value');
    }

    public function testDecreaseRejectsUnsignedBigIntegerUnderflow(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => 0,
        ]);
        $this->setupCollectionWithDocument('testCol', $doc, [
            $this->numericAttribute('value', ColumnType::BigInteger, false),
        ]);

        $this->expectException(LimitException::class);
        $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'value');
    }

    public function testUnsignedBigIntegerCrossesPhpIntegerBoundaryExactly(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => PHP_INT_MAX,
        ]);
        $this->setupCollectionWithDocument('testCol', $doc, [
            $this->numericAttribute('value', ColumnType::BigInteger, false),
        ]);

        $increased = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'value');
        $this->assertSame('9223372036854775808', $increased->getAttribute('value'));

        $decreased = $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'value');
        $this->assertSame(PHP_INT_MAX, $decreased->getAttribute('value'));
    }

    public function testUnsignedBigIntegerReachesExactMaximum(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => '18446744073709551614',
        ]);
        $this->setupCollectionWithDocument('testCol', $doc, [
            $this->numericAttribute('value', ColumnType::BigInteger, false),
        ]);

        $result = $this->database->increaseDocumentAttribute(
            'testCol',
            'doc1',
            'value',
            1,
            '18446744073709551615',
        );

        $this->assertSame('18446744073709551615', $result->getAttribute('value'));
    }

    public function testUnsignedBigIntegerRejectsOverflowAboveExactMaximum(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'value' => '18446744073709551615',
        ]);
        $this->setupCollectionWithDocument('testCol', $doc, [
            $this->numericAttribute('value', ColumnType::BigInteger, false),
        ]);

        $this->expectException(LimitException::class);
        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'value');
    }

    public function testIncreaseDocumentAttributeWithMax(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 8,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', 1, 10);
        $this->assertSame(9, $result->getAttribute('counter'));
    }

    public function testIncreaseDocumentAttributeExceedsMax(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 10,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->expectException(LimitException::class);
        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', 1, 10);
    }

    public function testIncreaseDocumentAttributeWithZeroValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be numeric and greater than 0');

        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', 0);
    }

    public function testIncreaseDocumentAttributeWithNegativeValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be numeric and greater than 0');

        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->database->increaseDocumentAttribute('testCol', 'doc1', 'counter', -1);
    }

    public function testIncreaseDocumentAttributeNotFound(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->expectException(NotFoundException::class);
        $this->database->increaseDocumentAttribute('testCol', 'nonexistent', 'counter');
    }

    public function testDecreaseDocumentAttribute(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 10,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter');
        $this->assertSame(9, $result->getAttribute('counter'));
    }

    public function testDecreaseDocumentAttributeWithMin(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $result = $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter', 1, 0);
        $this->assertSame(4, $result->getAttribute('counter'));
    }

    public function testDecreaseDocumentAttributeExceedsMin(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 3,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->expectException(LimitException::class);
        $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter', 5, 0);
    }

    public function testDecreaseDocumentAttributeWithZeroValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Value must be numeric and greater than 0');

        $doc = new Document([
            '$id' => 'doc1',
            '$collection' => 'testCol',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'counter' => 5,
        ]);

        $this->setupCollectionWithDocument('testCol', $doc, [$this->intAttribute('counter')]);

        $this->database->decreaseDocumentAttribute('testCol', 'doc1', 'counter', 0);
    }
}
