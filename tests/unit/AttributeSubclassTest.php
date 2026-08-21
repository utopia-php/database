<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionParameter;
use Utopia\Cache\Adapter\None as NoneAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Attribute\ArrayType;
use Utopia\Database\Attribute\BigInteger;
use Utopia\Database\Attribute\BigSerial;
use Utopia\Database\Attribute\Binary;
use Utopia\Database\Attribute\Boolean;
use Utopia\Database\Attribute\Datetime;
use Utopia\Database\Attribute\Decimal;
use Utopia\Database\Attribute\Double;
use Utopia\Database\Attribute\EnumType;
use Utopia\Database\Attribute\FloatType;
use Utopia\Database\Attribute\Id;
use Utopia\Database\Attribute\Integer;
use Utopia\Database\Attribute\Json;
use Utopia\Database\Attribute\Linestring;
use Utopia\Database\Attribute\LongText;
use Utopia\Database\Attribute\MediumText;
use Utopia\Database\Attribute\ObjectType;
use Utopia\Database\Attribute\Point;
use Utopia\Database\Attribute\Polygon;
use Utopia\Database\Attribute\Relationship;
use Utopia\Database\Attribute\Serial;
use Utopia\Database\Attribute\SmallInteger;
use Utopia\Database\Attribute\SmallSerial;
use Utopia\Database\Attribute\StringType;
use Utopia\Database\Attribute\Text;
use Utopia\Database\Attribute\Timestamp;
use Utopia\Database\Attribute\TinyInteger;
use Utopia\Database\Attribute\Tuple;
use Utopia\Database\Attribute\Uuid;
use Utopia\Database\Attribute\Uuid7;
use Utopia\Database\Attribute\Varchar;
use Utopia\Database\Attribute\Vector;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class AttributeSubclassTest extends TestCase
{
    /**
     * @return array<string, array{class-string<Attribute>, string, ColumnType, int}>
     */
    public static function types(): array
    {
        return [
            'string' => [StringType::class, 'string', ColumnType::String, Database::LENGTH_KEY],
            'varchar' => [Varchar::class, 'varchar', ColumnType::Varchar, Database::LENGTH_KEY],
            'text' => [Text::class, 'text', ColumnType::Text, 0],
            'mediumText' => [MediumText::class, 'mediumText', ColumnType::MediumText, 0],
            'longText' => [LongText::class, 'longText', ColumnType::LongText, 0],
            'tinyInteger' => [TinyInteger::class, 'tinyInteger', ColumnType::TinyInteger, 0],
            'smallInteger' => [SmallInteger::class, 'smallInteger', ColumnType::SmallInteger, 0],
            'integer' => [Integer::class, 'integer', ColumnType::Integer, 0],
            'bigInteger' => [BigInteger::class, 'bigInteger', ColumnType::BigInteger, 0],
            'float' => [FloatType::class, 'float', ColumnType::Float, 0],
            'double' => [Double::class, 'double', ColumnType::Double, 0],
            'decimal' => [Decimal::class, 'decimal', ColumnType::Decimal, 0],
            'boolean' => [Boolean::class, 'boolean', ColumnType::Boolean, 0],
            'datetime' => [Datetime::class, 'datetime', ColumnType::Datetime, 0],
            'timestamp' => [Timestamp::class, 'timestamp', ColumnType::Timestamp, 0],
            'json' => [Json::class, 'json', ColumnType::Json, 0],
            'binary' => [Binary::class, 'binary', ColumnType::Binary, 0],
            'enum' => [EnumType::class, 'enum', ColumnType::Enum, 0],
            'point' => [Point::class, 'point', ColumnType::Point, 0],
            'linestring' => [Linestring::class, 'linestring', ColumnType::Linestring, 0],
            'polygon' => [Polygon::class, 'polygon', ColumnType::Polygon, 0],
            'vector' => [Vector::class, 'vector', ColumnType::Vector, 0],
            'id' => [Id::class, 'id', ColumnType::Id, 0],
            'uuid' => [Uuid::class, 'uuid', ColumnType::Uuid, 0],
            'uuid7' => [Uuid7::class, 'uuid7', ColumnType::Uuid7, 0],
            'object' => [ObjectType::class, 'object', ColumnType::Object, 0],
            'relationship' => [Relationship::class, 'relationship', ColumnType::Relationship, 0],
            'serial' => [Serial::class, 'serial', ColumnType::Serial, 0],
            'bigSerial' => [BigSerial::class, 'bigSerial', ColumnType::BigSerial, 0],
            'smallSerial' => [SmallSerial::class, 'smallSerial', ColumnType::SmallSerial, 0],
            'array' => [ArrayType::class, 'array', ColumnType::Array, 0],
            'tuple' => [Tuple::class, 'tuple', ColumnType::Tuple, 0],
        ];
    }

    /**
     * @param class-string<Attribute> $class
     */
    #[DataProvider('types')]
    public function testFactoryAndSubclassDefaults(
        string $class,
        string $factory,
        ColumnType $type,
        int $defaultSize,
    ): void {
        $fromFactory = Attribute::{$factory}(key: 'x');
        $this->assertInstanceOf($class, $fromFactory);
        $this->assertSame($type, $fromFactory->type);
        $this->assertSame($defaultSize, $fromFactory->size);
        $this->assertSame('x', $fromFactory->key);

        $fromConstructor = new $class(key: 'x');
        $this->assertInstanceOf($class, $fromConstructor);
        $this->assertSame($type, $fromConstructor->type);
        $this->assertSame($defaultSize, $fromConstructor->size);
        $this->assertSame('x', $fromConstructor->key);
    }

    public function testStringFactoryOmitsTypeAndUsesLengthKey(): void
    {
        $attribute = Attribute::string(key: 'name');

        $this->assertInstanceOf(StringType::class, $attribute);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(Database::LENGTH_KEY, $attribute->size);
        $this->assertSame('name', $attribute->key);
        $this->assertSame(false, in_array('type', $this->parameterNames(Attribute::class, 'string'), true));
    }

    public function testStringTypeConstructorUsesLengthKey(): void
    {
        $attribute = new StringType(key: 'name');

        $this->assertInstanceOf(StringType::class, $attribute);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(Database::LENGTH_KEY, $attribute->size);
        $this->assertSame('name', $attribute->key);
        $this->assertSame(false, in_array('type', $this->parameterNames(StringType::class, '__construct'), true));
    }

    public function testIntegerConstructorOmitsType(): void
    {
        $attribute = new Integer(key: 'age', default: 0);

        $this->assertInstanceOf(Integer::class, $attribute);
        $this->assertSame(ColumnType::Integer, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame(0, $attribute->default);
        $this->assertSame('age', $attribute->key);
        $this->assertSame(false, in_array('type', $this->parameterNames(Integer::class, '__construct'), true));
    }

    public function testIntegerFactory(): void
    {
        $attribute = Attribute::integer(key: 'age', default: 0);

        $this->assertInstanceOf(Integer::class, $attribute);
        $this->assertSame(ColumnType::Integer, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame(0, $attribute->default);
        $this->assertSame('age', $attribute->key);
        $this->assertSame(false, in_array('type', $this->parameterNames(Attribute::class, 'integer'), true));
    }

    public function testVectorKeepsExplicitSize(): void
    {
        $fromFactory = Attribute::vector(key: 'embedding', size: 3);
        $this->assertInstanceOf(Vector::class, $fromFactory);
        $this->assertSame(ColumnType::Vector, $fromFactory->type);
        $this->assertSame(3, $fromFactory->size);
        $this->assertSame('embedding', $fromFactory->key);

        $fromConstructor = new Vector(key: 'embedding', size: 3);
        $this->assertInstanceOf(Vector::class, $fromConstructor);
        $this->assertSame(ColumnType::Vector, $fromConstructor->type);
        $this->assertSame(3, $fromConstructor->size);
        $this->assertSame('embedding', $fromConstructor->key);
    }

    public function testRelationshipStoresOptions(): void
    {
        $options = [
            'relatedCollection' => 'users',
            'relationType' => 'oneToMany',
            'twoWay' => true,
            'twoWayKey' => 'posts',
        ];

        $attribute = Attribute::relationship(key: 'author', options: $options);

        $this->assertInstanceOf(Relationship::class, $attribute);
        $this->assertSame(ColumnType::Relationship, $attribute->type);
        $this->assertSame($options, $attribute->options);
        $this->assertSame('author', $attribute->key);
        $this->assertSame(0, $attribute->size);
    }

    /**
     * @param class-string<Attribute> $class
     */
    #[DataProvider('types')]
    public function testFromDocumentReturnsSubclass(
        string $class,
        string $_factory,
        ColumnType $type,
        int $_defaultSize,
    ): void {
        $attribute = Attribute::fromDocument(new Document([
            'key' => 'x',
            'type' => $type->value,
        ]));

        $this->assertInstanceOf($class, $attribute);
        $this->assertSame($type, $attribute->type);
        $this->assertSame('x', $attribute->key);
    }

    /**
     * @param class-string<Attribute> $class
     */
    #[DataProvider('types')]
    public function testFromArrayReturnsSubclass(
        string $class,
        string $_factory,
        ColumnType $type,
        int $_defaultSize,
    ): void {
        $attribute = Attribute::fromArray([
            'key' => 'x',
            'type' => $type->value,
        ]);

        $this->assertInstanceOf($class, $attribute);
        $this->assertSame($type, $attribute->type);
        $this->assertSame('x', $attribute->key);
    }

    public function testFromDocumentPreservesStoredSizeZero(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            'key' => 'name',
            'type' => ColumnType::String->value,
            'size' => 0,
        ]));

        $this->assertInstanceOf(StringType::class, $attribute);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame('name', $attribute->key);
    }

    public function testFromArrayPreservesStoredSizeZero(): void
    {
        $attribute = Attribute::fromArray([
            'key' => 'name',
            'type' => ColumnType::String->value,
            'size' => 0,
        ]);

        $this->assertInstanceOf(StringType::class, $attribute);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame('name', $attribute->key);
    }

    /**
     * @param class-string<Attribute> $class
     */
    #[DataProvider('types')]
    public function testCreateDocumentInstanceHydratesMappedSubclass(
        string $class,
        string $_factory,
        ColumnType $type,
        int $_defaultSize,
    ): void {
        $database = $this->database();
        $database->setDocumentType('schema', $class);

        $document = $this->instantiate($database, 'schema', [
            '$id' => 'x',
            'key' => 'x',
            'type' => $type->value,
        ]);

        $this->assertInstanceOf($class, $document);
        $this->assertSame($type, $document->type);
        $this->assertSame('x', $document->key);
    }

    /**
     * @param class-string<Attribute> $class
     */
    #[DataProvider('types')]
    public function testCreateDocumentInstanceHydratesSubclassFromAttributeType(
        string $class,
        string $_factory,
        ColumnType $type,
        int $_defaultSize,
    ): void {
        $database = $this->database();
        $database->setDocumentType('schema', Attribute::class);

        $document = $this->instantiate($database, 'schema', [
            '$id' => 'x',
            'key' => 'x',
            'type' => $type->value,
        ]);

        $this->assertInstanceOf($class, $document);
        $this->assertSame($type, $document->type);
        $this->assertSame('x', $document->key);
    }

    public function testCreateDocumentInstanceUsesStoredTypeNotMappedClass(): void
    {
        $database = $this->database();
        $database->setDocumentType('schema', StringType::class);

        $document = $this->instantiate($database, 'schema', [
            '$id' => 'age',
            'key' => 'age',
            'type' => ColumnType::Integer->value,
        ]);

        $this->assertInstanceOf(Integer::class, $document);
    }

    public function testFromDocumentMissingSizeIsZero(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            'key' => 'name',
            'type' => ColumnType::String->value,
        ]));

        $this->assertInstanceOf(StringType::class, $attribute);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame('name', $attribute->key);
    }

    public function testLegacyBigintHydratesToBigInteger(): void
    {
        $attribute = Attribute::fromDocument(new Document([
            'key' => 'count',
            'type' => Attribute::LEGACY_BIG_INTEGER,
        ]));

        $this->assertInstanceOf(BigInteger::class, $attribute);
        $this->assertSame(ColumnType::BigInteger, $attribute->type);
        $this->assertSame('count', $attribute->key);
    }

    public function testBaseConstructorIsNotASubclass(): void
    {
        $attribute = new Attribute(key: 'name', type: ColumnType::String, size: Database::LENGTH_KEY);

        $this->assertSame(Attribute::class, $attribute::class);
        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(Database::LENGTH_KEY, $attribute->size);
        $this->assertSame('name', $attribute->key);
    }

    public function testStringRoundTrip(): void
    {
        $original = Attribute::string(key: 'email', size: 256, required: true);
        $restored = Attribute::fromDocument($original->toDocument());

        $this->assertInstanceOf(StringType::class, $restored);
        $this->assertSame($original->key, $restored->key);
        $this->assertSame($original->size, $restored->size);
        $this->assertSame($original->required, $restored->required);
        $this->assertSame($original->type, $restored->type);
        $this->assertSame(256, $restored->size);
        $this->assertSame(true, $restored->required);
    }

    public function testEnumAndArrayAreSubclasses(): void
    {
        $enum = Attribute::enum(key: 'status');
        $array = Attribute::array(key: 'tags');

        $this->assertInstanceOf(EnumType::class, $enum);
        $this->assertSame(ColumnType::Enum, $enum->type);
        $this->assertSame('status', $enum->key);
        $this->assertSame(0, $enum->size);

        $this->assertInstanceOf(ArrayType::class, $array);
        $this->assertSame(ColumnType::Array, $array->type);
        $this->assertSame('tags', $array->key);
        $this->assertSame(0, $array->size);
    }

    /**
     * @param class-string $class
     * @return array<int, string>
     */
    private function parameterNames(string $class, string $method): array
    {
        return array_map(
            static fn (ReflectionParameter $parameter): string => $parameter->getName(),
            (new ReflectionMethod($class, $method))->getParameters(),
        );
    }

    private function database(): Database
    {
        return new Database(
            $this->createStub(Adapter::class),
            new Cache(new NoneAdapter()),
        );
    }

    /**
     * @param array<string, mixed> $data
     */
    private function instantiate(Database $database, string $collection, array $data): Document
    {
        $method = new ReflectionMethod(Database::class, 'createDocumentInstance');

        /** @var Document $document */
        $document = $method->invoke($database, $collection, $data);

        return $document;
    }
}
