<?php

namespace Tests\Unit\Attributes;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Attribute as AttributeValidator;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;

final class TypeTableTest extends TestCase
{
    /**
     * @var list<ColumnType>
     */
    private const array STORABLE = [
        ColumnType::String,
        ColumnType::Varchar,
        ColumnType::Text,
        ColumnType::MediumText,
        ColumnType::LongText,
        ColumnType::Integer,
        ColumnType::BigInteger,
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::Boolean,
        ColumnType::Datetime,
        ColumnType::Id,
        ColumnType::Relationship,
        ColumnType::Object,
        ColumnType::Point,
        ColumnType::Linestring,
        ColumnType::Polygon,
        ColumnType::Vector,
    ];

    /**
     * @var list<ColumnType>
     */
    private const array UNSTORABLE = [
        ColumnType::TinyInteger,
        ColumnType::SmallInteger,
        ColumnType::Decimal,
        ColumnType::Timestamp,
        ColumnType::Json,
        ColumnType::Binary,
        ColumnType::Enum,
        ColumnType::Uuid,
        ColumnType::Uuid7,
        ColumnType::Serial,
        ColumnType::BigSerial,
        ColumnType::SmallSerial,
        ColumnType::Array,
        ColumnType::Tuple,
    ];

    /**
     * Factories whose type needs a capability the adapter does not have.
     *
     * @var array<class-string<Adapter>, array<string, string>>
     */
    private const array UNAVAILABLE = [
        Memory::class => [
            'point' => 'Spatial attributes are not supported',
            'linestring' => 'Spatial attributes are not supported',
            'polygon' => 'Spatial attributes are not supported',
            'vector' => 'Vector types are not supported by the current database',
        ],
        SQLite::class => [
            'object' => 'Object attributes are not supported',
            'point' => 'Spatial attributes are not supported',
            'linestring' => 'Spatial attributes are not supported',
            'polygon' => 'Spatial attributes are not supported',
            'vector' => 'Vector types are not supported by the current database',
        ],
    ];

    /**
     * @return array<string, TypeSample>
     */
    private static function samples(): array
    {
        return [
            'string' => new TypeSample(value: 'text', readType: 'string', default: 'fallback', size: 64),
            'varchar' => new TypeSample(value: 'text', readType: 'string', default: 'fallback', size: 64),
            'text' => new TypeSample(value: 'text', readType: 'string', default: 'fallback'),
            'mediumText' => new TypeSample(value: 'text', readType: 'string', default: 'fallback'),
            'longText' => new TypeSample(value: 'text', readType: 'string', default: 'fallback'),
            'integer' => new TypeSample(value: 5, readType: 'int', default: 1, incrementable: true),
            'bigInteger' => new TypeSample(value: 5, readType: 'int', default: 1, incrementable: true),
            'float' => new TypeSample(value: 1.5, readType: 'float', default: 0.5, incrementable: true),
            'double' => new TypeSample(value: 1.5, readType: 'float', default: 0.5, incrementable: true),
            'boolean' => new TypeSample(value: true, readType: 'bool', default: false),
            'datetime' => new TypeSample(value: '2024-01-01T00:00:00.000+00:00', readType: 'string', default: '2023-06-01T12:00:00.000+00:00'),
            'id' => new TypeSample(value: '7', readType: 'string'),
            'object' => new TypeSample(value: ['colour' => 'red'], readType: 'array', default: ['colour' => 'blue']),
            'point' => new TypeSample(value: [1.0, 2.0], readType: 'array'),
            'linestring' => new TypeSample(value: [[1.0, 2.0], [3.0, 4.0]], readType: 'array'),
            'polygon' => new TypeSample(value: [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0]]], readType: 'array'),
            'vector' => new TypeSample(value: [1.0, 2.0, 3.0], readType: 'array', size: 3),
        ];
    }

    /**
     * @return array<string, array{string, class-string<Adapter>}>
     */
    public static function availableFactories(): array
    {
        $cases = [];
        foreach (\array_keys(self::samples()) as $factory) {
            foreach (self::UNAVAILABLE as $adapter => $unavailable) {
                if (! \array_key_exists($factory, $unavailable)) {
                    $cases[$factory.' on '.(new ReflectionClass($adapter))->getShortName()] = [$factory, $adapter];
                }
            }
        }

        return $cases;
    }

    /**
     * @return array<string, array{string, class-string<Adapter>}>
     */
    public static function defaultedFactories(): array
    {
        return \array_filter(
            self::availableFactories(),
            fn (array $case): bool => self::samples()[$case[0]]->default !== null,
        );
    }

    /**
     * @return array<string, array{string, class-string<Adapter>, string}>
     */
    public static function unavailableFactories(): array
    {
        $cases = [];
        foreach (self::UNAVAILABLE as $adapter => $unavailable) {
            foreach ($unavailable as $factory => $message) {
                $cases[$factory.' on '.(new ReflectionClass($adapter))->getShortName()] = [$factory, $adapter, $message];
            }
        }

        return $cases;
    }

    /**
     * @return array<string, array{ColumnType, class-string<Adapter>}>
     */
    public static function unstorableTypes(): array
    {
        $cases = [];
        foreach (self::UNSTORABLE as $type) {
            foreach (\array_keys(self::UNAVAILABLE) as $adapter) {
                $cases[$type->value.' on '.(new ReflectionClass($adapter))->getShortName()] = [$type, $adapter];
            }
        }

        return $cases;
    }

    /**
     * @return array<string, array{class-string<Adapter>}>
     */
    public static function adapters(): array
    {
        $cases = [];
        foreach (\array_keys(self::UNAVAILABLE) as $adapter) {
            $cases[(new ReflectionClass($adapter))->getShortName()] = [$adapter];
        }

        return $cases;
    }

    /**
     * @return array<string, array{ColumnType}>
     */
    public static function columnTypes(): array
    {
        $cases = [];
        foreach (ColumnType::cases() as $type) {
            $cases[$type->value] = [$type];
        }

        return $cases;
    }

    public function testAvailableTypesFollowTheTableAndTheCapabilities(): void
    {
        $everywhere = [
            ColumnType::String,
            ColumnType::Varchar,
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText,
            ColumnType::Integer,
            ColumnType::BigInteger,
            ColumnType::Float,
            ColumnType::Double,
            ColumnType::Boolean,
            ColumnType::Datetime,
            ColumnType::Id,
            ColumnType::Relationship,
        ];

        $this->assertSame(self::STORABLE, Attribute::availableTypes(objects: true, spatial: true, vectors: true));
        $this->assertSame($everywhere, Attribute::availableTypes(objects: false, spatial: false, vectors: false));
        $this->assertSame([...$everywhere, ColumnType::Object], Attribute::availableTypes(objects: true, spatial: false, vectors: false));
        $this->assertSame(
            [...$everywhere, ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon],
            Attribute::availableTypes(objects: false, spatial: true, vectors: false),
        );
        $this->assertSame([...$everywhere, ColumnType::Vector], Attribute::availableTypes(objects: false, spatial: false, vectors: true));
    }

    #[DataProvider('columnTypes')]
    public function testEveryColumnTypeIsEitherStorableOrNot(ColumnType $type): void
    {
        $this->assertNotSame(
            \in_array($type, self::STORABLE, true),
            \in_array($type, self::UNSTORABLE, true),
            $type->value.' must be classified exactly once',
        );
    }

    #[DataProvider('columnTypes')]
    public function testCheckTypeFollowsTheTable(ColumnType $type): void
    {
        $validator = new AttributeValidator(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 16381,
            maxIntLength: 4294967295,
            supportForVectors: true,
            supportForSpatialAttributes: true,
            supportForObject: true,
        );
        $attribute = new Attribute(key: 'value', type: $type, size: $this->validSize($type));

        if (\in_array($type, self::STORABLE, true)) {
            $this->assertTrue($validator->checkType($attribute));

            return;
        }

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Unknown attribute type: '.$type->value.'.');
        $validator->checkType($attribute);
    }

    #[DataProvider('columnTypes')]
    public function testStructureFollowsTheTable(ColumnType $type): void
    {
        $attribute = new Attribute(key: 'value', type: $type, size: $this->validSize($type));
        $structure = new Structure(
            new Document([
                Document::ID => 'items',
                Document::COLLECTION => Database::METADATA,
                'attributes' => [$attribute->toDocument()],
            ]),
            ColumnType::Integer->value,
        );
        $document = new Document([
            Document::COLLECTION => 'items',
            Document::CREATED_AT => '2024-01-01T00:00:00.000+00:00',
            Document::UPDATED_AT => '2024-01-01T00:00:00.000+00:00',
            'value' => $this->validValue($type),
        ]);

        if (\in_array($type, self::STORABLE, true)) {
            $this->assertTrue($structure->isValid($document), $structure->getDescription());

            return;
        }

        $this->assertFalse($structure->isValid($document));
        $this->assertStringContainsString('Unknown attribute type "'.$type->value.'"', $structure->getDescription());
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('unstorableTypes')]
    public function testUnstorableTypesAreRejectedUpFront(ColumnType $type, string $adapter): void
    {
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));

        $message = 'Unknown attribute type: '.$type->value.'.';
        $this->assertRejected($message, fn () => $database->createAttribute('items', new Attribute(key: 'value', type: $type)));
        $this->assertRejected($message, fn () => $database->createAttributes('items', [new Attribute(key: 'value', type: $type)]));
        $this->assertRejected($message, fn () => $database->createCollection(new Collection(
            id: 'inline',
            attributes: [new Attribute(key: 'value', type: $type)],
            permissions: $this->permissions(),
        )));

        $this->assertTrue($database->getCollection('inline')->isEmpty());
        $this->assertSame([], $database->getCollection('items')->attributes);
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('unavailableFactories')]
    public function testTypesTheAdapterCannotStoreAreRejectedUpFront(string $factory, string $adapter, string $message): void
    {
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));

        $this->assertRejected($message, fn () => $database->createAttribute('items', $this->attribute($factory)));
        $this->assertRejected($message, fn () => $database->createCollection(new Collection(
            id: 'inline',
            attributes: [$this->attribute($factory)],
            permissions: $this->permissions(),
        )));

        $this->assertTrue($database->getCollection('inline')->isEmpty());
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('availableFactories')]
    public function testTypeRoundTripsThroughCreateAttribute(string $factory, string $adapter): void
    {
        $sample = self::samples()[$factory];
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));

        $this->assertTrue($database->createAttribute('items', $this->attribute($factory)));
        $this->write($database, ['value' => $sample->value]);

        $this->assertStored($sample->readType, $sample->value, $database->getDocument('items', 'one')->getAttribute('value'));
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('availableFactories')]
    public function testTypeRoundTripsThroughCreateCollection(string $factory, string $adapter): void
    {
        $sample = self::samples()[$factory];
        $database = $this->database($adapter);
        $database->createCollection(new Collection(
            id: 'items',
            attributes: [$this->attribute($factory)],
            permissions: $this->permissions(),
        ));

        $this->write($database, ['value' => $sample->value]);

        $this->assertStored($sample->readType, $sample->value, $database->getDocument('items', 'one')->getAttribute('value'));
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('availableFactories')]
    public function testTypeCanBeUpdated(string $factory, string $adapter): void
    {
        $sample = self::samples()[$factory];
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));
        $database->createAttribute('items', $this->attribute($factory));
        $this->write($database, ['value' => $sample->value]);

        $updated = $database->updateAttribute('items', 'value', newKey: 'renamed');

        $this->assertSame('renamed', $updated->getAttribute('key'));
        $this->assertSame(Attribute::normalizeType($this->attribute($factory)->type), Attribute::normalizeType($this->storedType($updated)));
        $this->assertStored($sample->readType, $sample->value, $database->getDocument('items', 'one')->getAttribute('renamed'));
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('defaultedFactories')]
    public function testTypeTakesADefault(string $factory, string $adapter): void
    {
        $sample = self::samples()[$factory];
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));
        $database->createAttribute('items', $this->attribute($factory, $sample->default));
        $this->write($database);

        $this->assertStored($sample->readType, $sample->default, $database->getDocument('items', 'one')->getAttribute('value'));

        $updated = $database->updateAttributeDefault('items', 'value', $sample->value);
        $this->assertSame($sample->value, $updated->getAttribute('default'));
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('adapters')]
    public function testDefaultOnATypeWithoutScalarDefaultsIsAMismatch(string $adapter): void
    {
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));
        $database->createAttribute('items', Attribute::id(key: 'value'));

        $this->assertRejected(
            'Default value 5 does not match given type id',
            fn () => $database->updateAttributeDefault('items', 'value', '5'),
        );
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    #[DataProvider('availableFactories')]
    public function testOnlyNumericTypesIncrement(string $factory, string $adapter): void
    {
        $sample = self::samples()[$factory];
        $database = $this->database($adapter);
        $database->createCollection(new Collection(id: 'items', permissions: $this->permissions()));
        $database->createAttribute('items', $this->attribute($factory));
        $this->write($database, ['value' => $sample->value]);

        if (! $sample->incrementable) {
            $this->expectException(TypeException::class);
            $database->increaseDocumentAttribute('items', 'one', 'value');

            return;
        }

        $this->assertIsNumeric($sample->value);
        $increased = $database->increaseDocumentAttribute('items', 'one', 'value', 2);
        $this->assertStored($sample->readType, $sample->value + 2, $increased->getAttribute('value'));

        $decreased = $database->decreaseDocumentAttribute('items', 'one', 'value', 2);
        $this->assertStored($sample->readType, $sample->value, $decreased->getAttribute('value'));
    }

    private function attribute(string $factory, mixed $default = null): Attribute
    {
        $sample = self::samples()[$factory];
        $attribute = Attribute::{$factory}(key: 'value', size: $sample->size, default: $default);
        $this->assertInstanceOf(Attribute::class, $attribute);

        return $attribute;
    }

    /**
     * @param  class-string<Adapter>  $adapter
     */
    private function database(string $adapter): Database
    {
        $database = new Database(
            $adapter === SQLite::class ? new SQLite(new PDO('sqlite::memory:')) : new Memory(),
            new Cache(new None()),
        );
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('type_table')
            ->setNamespace('type_table_'.\uniqid());
        $database->create();

        return $database;
    }

    /**
     * @return list<string>
     */
    private function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    /**
     * @param  array<string, mixed>  $values
     */
    private function write(Database $database, array $values = []): void
    {
        $database->createDocument('items', new Document([
            Document::ID => 'one',
            Document::PERMISSIONS => $this->permissions(),
            ...$values,
        ]));
    }

    private function assertStored(string $readType, mixed $expected, mixed $actual): void
    {
        $this->assertSame($readType, \get_debug_type($actual));
        $this->assertSame($expected, $actual);
    }

    private function storedType(Document $attribute): ColumnType|string
    {
        $type = $attribute->getAttribute('type');
        $this->assertTrue($type instanceof ColumnType || \is_string($type));

        return $type;
    }

    private function assertRejected(string $message, callable $operation): void
    {
        try {
            $operation();
        } catch (DatabaseException $error) {
            $this->assertStringContainsString($message, $error->getMessage());

            return;
        }

        $this->fail('Expected the operation to be rejected with "'.$message.'"');
    }

    private function validSize(ColumnType $type): int
    {
        return match ($type) {
            ColumnType::String, ColumnType::Varchar => 64,
            ColumnType::Vector => 3,
            default => 0,
        };
    }

    private function validValue(ColumnType $type): mixed
    {
        return match ($type) {
            ColumnType::Integer, ColumnType::BigInteger => 5,
            ColumnType::Float, ColumnType::Double => 1.5,
            ColumnType::Boolean => true,
            ColumnType::Datetime => '2024-01-01T00:00:00.000+00:00',
            ColumnType::Id => '7',
            ColumnType::Object => ['colour' => 'red'],
            ColumnType::Point => [1.0, 2.0],
            ColumnType::Linestring => [[1.0, 2.0], [3.0, 4.0]],
            ColumnType::Polygon => [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0]]],
            ColumnType::Vector => [1.0, 2.0, 3.0],
            ColumnType::TinyInteger, ColumnType::SmallInteger, ColumnType::Serial, ColumnType::BigSerial, ColumnType::SmallSerial => 5,
            default => 'text',
        };
    }
}
