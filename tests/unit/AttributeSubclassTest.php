<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoneAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Attribute\BigInteger;
use Utopia\Database\Attribute\Boolean;
use Utopia\Database\Attribute\Datetime;
use Utopia\Database\Attribute\Double;
use Utopia\Database\Attribute\FloatType;
use Utopia\Database\Attribute\Id;
use Utopia\Database\Attribute\Integer;
use Utopia\Database\Attribute\Linestring;
use Utopia\Database\Attribute\LongText;
use Utopia\Database\Attribute\MediumText;
use Utopia\Database\Attribute\ObjectType;
use Utopia\Database\Attribute\Point;
use Utopia\Database\Attribute\Polygon;
use Utopia\Database\Attribute\Relationship;
use Utopia\Database\Attribute\StringType;
use Utopia\Database\Attribute\Text;
use Utopia\Database\Attribute\Varchar;
use Utopia\Database\Attribute\Vector;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
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
            'integer' => [Integer::class, 'integer', ColumnType::Integer, 0],
            'bigInteger' => [BigInteger::class, 'bigInteger', ColumnType::BigInteger, 0],
            'float' => [FloatType::class, 'float', ColumnType::Float, 0],
            'double' => [Double::class, 'double', ColumnType::Double, 0],
            'boolean' => [Boolean::class, 'boolean', ColumnType::Boolean, 0],
            'datetime' => [Datetime::class, 'datetime', ColumnType::Datetime, 0],
            'point' => [Point::class, 'point', ColumnType::Point, 0],
            'linestring' => [Linestring::class, 'linestring', ColumnType::Linestring, 0],
            'polygon' => [Polygon::class, 'polygon', ColumnType::Polygon, 0],
            'vector' => [Vector::class, 'vector', ColumnType::Vector, 0],
            'id' => [Id::class, 'id', ColumnType::Id, 0],
            'object' => [ObjectType::class, 'object', ColumnType::Object, 0],
            'relationship' => [Relationship::class, 'relationship', ColumnType::Relationship, 0],
        ];
    }

    /**
     * @return array<string, array{ColumnType}>
     */
    public static function removedTypes(): array
    {
        return [
            'tinyinteger' => [ColumnType::TinyInteger],
            'smallinteger' => [ColumnType::SmallInteger],
            'decimal' => [ColumnType::Decimal],
            'timestamp' => [ColumnType::Timestamp],
            'json' => [ColumnType::Json],
            'binary' => [ColumnType::Binary],
            'enum' => [ColumnType::Enum],
            'uuid' => [ColumnType::Uuid],
            'uuid7' => [ColumnType::Uuid7],
            'serial' => [ColumnType::Serial],
            'bigserial' => [ColumnType::BigSerial],
            'smallserial' => [ColumnType::SmallSerial],
            'array' => [ColumnType::Array],
            'tuple' => [ColumnType::Tuple],
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

        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(Database::LENGTH_KEY, $attribute->size);
        $this->assertSame('name', $attribute->key);
    }

    public function testStringTypeConstructorUsesLengthKey(): void
    {
        $attribute = new StringType(key: 'name');

        $this->assertSame(ColumnType::String, $attribute->type);
        $this->assertSame(Database::LENGTH_KEY, $attribute->size);
        $this->assertSame('name', $attribute->key);
    }

    public function testIntegerConstructorOmitsType(): void
    {
        $attribute = new Integer(key: 'age', default: 0);

        $this->assertSame(ColumnType::Integer, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame(0, $attribute->default);
        $this->assertSame('age', $attribute->key);
    }

    public function testIntegerFactory(): void
    {
        $attribute = Attribute::integer(key: 'age', default: 0);

        $this->assertSame(ColumnType::Integer, $attribute->type);
        $this->assertSame(0, $attribute->size);
        $this->assertSame(0, $attribute->default);
        $this->assertSame('age', $attribute->key);
    }

    public function testVectorKeepsExplicitSize(): void
    {
        $fromFactory = Attribute::vector(key: 'embedding', size: 3);
        $this->assertSame(ColumnType::Vector, $fromFactory->type);
        $this->assertSame(3, $fromFactory->size);
        $this->assertSame('embedding', $fromFactory->key);

        $fromConstructor = new Vector(key: 'embedding', size: 3);
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
    public function testReadingBackHydratesTheMappedSubclass(
        string $class,
        string $_factory,
        ColumnType $type,
        int $_defaultSize,
    ): void {
        $document = $this->store($class, $type);

        $this->assertInstanceOf($class, $document);
        $this->assertSame($type, $document->type);
        $this->assertSame('x', $document->key);
    }

    /**
     * @param class-string<Attribute> $class
     */
    #[DataProvider('types')]
    public function testReadingBackHydratesTheSubclassThatTheStoredTypeNames(
        string $class,
        string $_factory,
        ColumnType $type,
        int $_defaultSize,
    ): void {
        $document = $this->store(Attribute::class, $type);

        $this->assertInstanceOf($class, $document);
        $this->assertSame($type, $document->type);
        $this->assertSame('x', $document->key);
    }

    public function testStoredTypeWinsOverTheMappedClass(): void
    {
        $this->assertInstanceOf(Integer::class, $this->store(StringType::class, ColumnType::Integer));
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
            'type' => 'bigint',
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

    #[DataProvider('removedTypes')]
    public function testMetadataOfARemovedTypeStillHydratesAsThePlainAttribute(ColumnType $type): void
    {
        $fromDocument = Attribute::fromDocument(new Document(['key' => 'x', 'type' => $type->value]));
        $fromArray = Attribute::fromArray(['key' => 'x', 'type' => $type->value]);

        foreach ([$fromDocument, $fromArray] as $attribute) {
            $this->assertSame(Attribute::class, $attribute::class);
            $this->assertSame($type, $attribute->type);
            $this->assertSame('x', $attribute->key);
        }
    }

    /**
     * Write one attribute row and read it back through the public API, under
     * the given document type mapping.
     *
     * @param  class-string<Attribute>  $documentType
     */
    private function store(string $documentType, ColumnType $type): Document
    {
        $database = new Database(new Memory(), new Cache(new NoneAdapter()));
        $database->setAuthorization(new Authorization())
            ->setDatabase('attribute_subclass')
            ->setNamespace('subclass_'.\uniqid());
        $database->create();

        $database->createCollection(new Collection(id: 'schema', attributes: [
            Attribute::string(key: 'key', size: 255),
            Attribute::string(key: 'type', size: 64),
        ], permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));

        $database->setDocumentType('schema', $documentType);

        $database->createDocument('schema', new Document([
            '$id' => 'x',
            '$permissions' => [Permission::read(Role::any())],
            'key' => 'x',
            'type' => $type->value,
        ]));

        return $database->getDocument('schema', 'x');
    }
}
