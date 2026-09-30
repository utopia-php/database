<?php

namespace Tests\Unit\Attributes;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
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
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Text;

final class UpdateAttributeValidationTest extends TestCase
{
    private const string COLLECTION = 'items';

    private const string FORMAT = 'updateAttributeIntegerOnly';

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
     * @return array<string, \Closure(Database, string, string): mixed>
     */
    private static function updaters(): array
    {
        return [
            'required' => static fn (Database $database, string $collection, string $id): mixed => $database->updateAttributeRequired($collection, $id, false),
            'format' => static fn (Database $database, string $collection, string $id): mixed => $database->updateAttributeFormat($collection, $id, 'text'),
            'format options' => static fn (Database $database, string $collection, string $id): mixed => $database->updateAttributeFormatOptions($collection, $id, ['maximum' => 1]),
            'filters' => static fn (Database $database, string $collection, string $id): mixed => $database->updateAttributeFilters($collection, $id, []),
            'default' => static fn (Database $database, string $collection, string $id): mixed => $database->updateAttributeDefault($collection, $id, 'x'),
            'structure' => static fn (Database $database, string $collection, string $id): mixed => $database->updateAttribute($collection, $id, size: 128),
        ];
    }

    /**
     * @return array<string, array{\Closure(): Adapter, \Closure(Database, string, string): mixed}>
     */
    public static function updatersOverAdapters(): array
    {
        $cases = [];
        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach (self::updaters() as $updaterName => $updater) {
                $cases["{$updaterName} on {$adapterName}"] = [$adapter, $updater];
            }
        }

        return $cases;
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     * @param  \Closure(Database, string, string): mixed  $updater
     */
    #[DataProvider('updatersOverAdapters')]
    public function testAnUpdateOfTheMetadataCollectionIsRefused(\Closure $adapter, \Closure $updater): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database, Database::METADATA);

        try {
            $updater($database, Database::METADATA, 'name');
            $this->fail('the metadata collection must not be updated');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot update metadata attributes', $error->getMessage());
        }

        $this->assertSame($before, $this->definitions($database, Database::METADATA));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     * @param  \Closure(Database, string, string): mixed  $updater
     */
    #[DataProvider('updatersOverAdapters')]
    public function testAnUpdateOfAnUnknownAttributeIsNotFound(\Closure $adapter, \Closure $updater): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database);

        try {
            $updater($database, self::COLLECTION, 'missing');
            $this->fail('an unknown attribute must not be updated');
        } catch (NotFoundException $error) {
            $this->assertSame('Attribute not found', $error->getMessage());
        }

        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAFormatOfAnotherTypeIsRefused(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        Structure::addFormat(self::FORMAT, static fn (mixed $attribute): Text => new Text(0), ColumnType::Integer);

        try {
            $before = $this->definitions($database);
            $this->assertRefused(
                'Format "'.self::FORMAT.'" not available for attribute type "string"',
                fn (): mixed => $database->updateAttributeFormat(self::COLLECTION, 'label', self::FORMAT),
            );
            $this->assertRefused(
                'Format ("'.self::FORMAT.'") not available for this attribute type ("string")',
                fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', format: self::FORMAT),
            );
            $this->assertSame($before, $this->definitions($database));

            $this->assertSame(self::FORMAT, $database->updateAttributeFormat(self::COLLECTION, 'count', self::FORMAT)->getAttribute('format'));
        } finally {
            Structure::removeFormat(self::FORMAT);
        }
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testADefaultOnARequiredAttributeIsRefused(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database);

        foreach (['x', null] as $default) {
            $this->assertRefused(
                'Cannot set a default value on a required attribute',
                fn (): mixed => $database->updateAttributeDefault(self::COLLECTION, 'name', $default),
            );
        }

        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @return array<string, array{\Closure(): Adapter, string, mixed, string}>
     */
    public static function mismatchedDefaults(): array
    {
        $defaults = [
            'string given an integer' => ['label', 123, 'Default value 123 does not match given type string'],
            'integer given a string' => ['count', 'abc', 'Default value abc does not match given type integer'],
            'boolean given an integer' => ['flag', 1, 'Default value 1 does not match given type boolean'],
            'float given a string' => ['ratio', 'x', 'Default value x does not match given type float'],
            'datetime given an integer' => ['occurredAt', 5, 'Default value 5 does not match given type datetime'],
            'string array given a list with an integer' => ['tags', ['a', 1], 'Default value 1 does not match given type string'],
        ];

        $cases = [];
        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach ($defaults as $name => [$attribute, $default, $message]) {
                $cases["{$name} on {$adapterName}"] = [$adapter, $attribute, $default, $message];
            }
        }

        return $cases;
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('mismatchedDefaults')]
    public function testAnUpdatedDefaultOfTheWrongTypeIsAMismatch(\Closure $adapter, string $attribute, mixed $default, string $message): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database);

        $this->assertRefused($message, fn (): mixed => $database->updateAttributeDefault(self::COLLECTION, $attribute, $default));

        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnArrayDefaultOfTheRightTypeIsAccepted(\Closure $adapter): void
    {
        $database = $this->database($adapter());

        $this->assertSame(['a', 'b'], $database->updateAttributeDefault(self::COLLECTION, 'tags', ['a', 'b'])->getAttribute('default'));
        $this->assertSame(['a', 'b'], $this->definitions($database)['tags']['default']);
    }

    public function testAnUpdatedVectorDefaultNeedsNumericComponents(): void
    {
        $database = $this->database($this->vectorMemory());
        $database->createAttribute(self::COLLECTION, Attribute::vector(key: 'embedding', size: 3));
        $before = $this->definitions($database);

        $this->assertRefused(
            'Vector components must be numeric values (float or integer)',
            fn (): mixed => $database->updateAttributeDefault(self::COLLECTION, 'embedding', ['a', 'b', 'c']),
        );

        $this->assertSame($before, $this->definitions($database));
        $this->assertSame([1, 2.5, 3], $database->updateAttributeDefault(self::COLLECTION, 'embedding', [1, 2.5, 3])->getAttribute('default'));
    }

    /**
     * @return array<string, array{\Closure(): Adapter, ColumnType}>
     */
    public static function unstorableTypes(): array
    {
        $cases = [];
        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach ([ColumnType::Json, ColumnType::Decimal, ColumnType::Uuid, ColumnType::Tuple] as $type) {
                $cases["{$type->value} on {$adapterName}"] = [$adapter, $type];
            }
        }

        return $cases;
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('unstorableTypes')]
    public function testATypeTheAdapterCannotStoreIsAnUnknownTypeOnUpdate(\Closure $adapter, ColumnType $type): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database);

        $message = $this->refusal(fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: $type));

        $this->assertStringStartsWith("Unknown attribute type: {$type->value}. Must be one of ", $message);
        $listed = \explode(', ', \substr($message, \strlen("Unknown attribute type: {$type->value}. Must be one of ")));
        $this->assertContains(ColumnType::String->value, $listed);
        $this->assertContains(ColumnType::Relationship->value, $listed);
        $this->assertSame(
            $database->getAdapter()->supports(Capability::Objects),
            \in_array(ColumnType::Object->value, $listed, true),
            'object is listed exactly when the adapter stores objects',
        );
        $this->assertNotContains(ColumnType::Point->value, $listed);
        $this->assertNotContains(ColumnType::Vector->value, $listed);
        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testARelationshipCannotBeUpdatedAsAnAttribute(\Closure $adapter): void
    {
        $database = $this->database($adapter());

        $this->assertRefused(
            'Cannot update relationship as an attribute',
            fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Relationship),
        );
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheSizeRulesApplyOnUpdate(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        $limits = $database->getAdapter();
        $before = $this->definitions($database);

        $this->assertRefused('Size length is required', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', size: 0));
        $this->assertRefused(
            'Max size allowed for string is: '.\number_format($limits->getLimitForString()),
            fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', size: $limits->getLimitForString() + 1),
        );
        $this->assertRefused('Size length is required', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Varchar, size: 0));
        $this->assertRefused(
            'Max size allowed for varchar is: '.\number_format($limits->getMaxVarcharLength()),
            fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Varchar, size: $limits->getMaxVarcharLength() + 1),
        );
        $signedLimit = $limits->getLimitForInt() / 2;
        $this->assertRefused(
            'Max size allowed for int is: '.\number_format($signedLimit),
            fn (): mixed => $database->updateAttribute(self::COLLECTION, 'count', size: (int) $signedLimit + 1),
        );
        $this->assertRefused('Size must be empty', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'ratio', size: 8));
        $this->assertRefused('Size must be empty', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'flag', size: 1));

        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheObjectRulesApplyOnUpdate(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database);

        if (! $database->getAdapter()->supports(Capability::Objects)) {
            $this->assertRefused('Object attributes are not supported', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Object, size: 0));
            $this->assertSame($before, $this->definitions($database));

            return;
        }

        $this->assertRefused('Size must be empty for object attributes', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Object));
        $this->assertRefused('Object attributes cannot be arrays', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Object, size: 0, array: true));
        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testSpatialAndVectorTypesNeedTheirSupportOnUpdate(\Closure $adapter): void
    {
        $database = $this->database($adapter());

        foreach ([ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon] as $spatial) {
            $this->assertRefused('Spatial attributes are not supported', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: $spatial, size: 0));
        }
        $this->assertRefused(
            'Vector types are not supported by the current database',
            fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', type: ColumnType::Vector, size: 3),
        );
    }

    /**
     * @return array<string, array{array<string, mixed>, string}>
     */
    public static function invalidVectorDefinitions(): array
    {
        return [
            'an array' => [['array' => true], 'Vector type cannot be an array'],
            'no dimensions' => [['size' => 0], 'Vector dimensions must be a positive integer'],
            'too many dimensions' => [['size' => Database::MAX_VECTOR_DIMENSIONS + 1], 'Vector dimensions cannot exceed '.Database::MAX_VECTOR_DIMENSIONS],
            'a scalar default' => [['default' => 'x'], 'Vector default value must be an array'],
            'a default of the wrong length' => [['default' => [1.0, 2.0]], 'Vector default value must have exactly 3 elements'],
            'a non-numeric default' => [['default' => [1.0, 'a', 2.0]], 'Vector default value must contain only numeric elements'],
        ];
    }

    /**
     * @param  array<string, mixed>  $change
     */
    #[DataProvider('invalidVectorDefinitions')]
    public function testTheVectorRulesApplyOnUpdate(array $change, string $message): void
    {
        $database = $this->database($this->vectorMemory());
        $database->createAttribute(self::COLLECTION, Attribute::vector(key: 'embedding', size: 3));
        $before = $this->definitions($database);

        /** @var int|null $size */
        $size = $change['size'] ?? null;
        /** @var bool|null $array */
        $array = $change['array'] ?? null;
        $this->assertRefused($message, fn (): mixed => $database->updateAttribute(
            self::COLLECTION,
            'embedding',
            size: $size,
            default: $change['default'] ?? null,
            array: $array,
        ));

        $this->assertSame($before, $this->definitions($database));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testADatetimeAttributeKeepsItsRequiredFilterOnUpdate(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        $before = $this->definitions($database);

        $this->assertRefused(
            'Attribute of type: datetime requires the following filters: datetime',
            fn (): mixed => $database->updateAttribute(self::COLLECTION, 'occurredAt', filters: []),
        );
        $this->assertSame($before, $this->definitions($database));
    }

    public function testAnUpdatePastTheRowWidthLimitIsRefused(): void
    {
        $database = $this->database(new class () extends Memory {
            public function getDocumentSizeLimit(): int
            {
                return 1_000;
            }

            public function getAttributeWidth(Document $collection): int
            {
                return 1_000;
            }
        });
        $before = $this->definitions($database);

        try {
            $database->updateAttribute(self::COLLECTION, 'label', size: 128);
            $this->fail('an update past the row width limit must be refused');
        } catch (LimitException $error) {
            $this->assertSame('Row width limit reached. Cannot update attribute.', $error->getMessage());
        }

        $this->assertSame($before, $this->definitions($database));
    }

    public function testAnAdapterThatDoesNotUpdateTheColumnFailsTheUpdate(): void
    {
        $database = $this->database(new class () extends Memory {
            public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
            {
                return false;
            }
        });
        $before = $this->definitions($database);

        $this->assertRefused('Failed to update attribute', fn (): mixed => $database->updateAttribute(self::COLLECTION, 'label', size: 128));
        $this->assertSame($before, $this->definitions($database));
    }

    public function testARenameTheAdapterDoesNotApplyIsReportedWithBothNames(): void
    {
        $database = $this->database(new class () extends Memory {
            public function renameAttribute(string $collection, string $old, string $new): bool
            {
                return false;
            }
        });
        $before = $this->definitions($database);

        $this->assertRefused(
            "Failed to rename attribute 'label' to 'caption': Failed to rename attribute",
            fn (): bool => $database->renameAttribute(self::COLLECTION, 'label', 'caption'),
        );
        $this->assertSame($before, $this->definitions($database));
    }

    public function testARenameFailureOnASchemaIntrospectingAdapterIsWrappedWithTheCause(): void
    {
        $cause = new RuntimeException('the engine refused the rename');
        $database = $this->database(new class (new PDO('sqlite::memory:'), $cause) extends SQLite {
            public function __construct(object $pdo, private readonly RuntimeException $cause)
            {
                parent::__construct($pdo);
            }

            public function renameAttribute(string $collection, string $old, string $new): bool
            {
                throw $this->cause;
            }
        });
        $before = $this->definitions($database);

        try {
            $database->renameAttribute(self::COLLECTION, 'label', 'caption');
            $this->fail('a failed rename must be reported');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to rename attribute 'label' to 'caption': the engine refused the rename", $error->getMessage());
            $this->assertSame($cause, $error->getPrevious());
        }

        $this->assertSame($before, $this->definitions($database));
    }

    private function vectorMemory(): Memory
    {
        return new class () extends Memory {
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::Vectors];
            }
        };
    }

    /**
     * @param  callable(): mixed  $update
     */
    private function assertRefused(string $message, callable $update): void
    {
        $this->assertSame($message, $this->refusal($update));
    }

    /**
     * @param  callable(): mixed  $update
     */
    private function refusal(callable $update): string
    {
        try {
            $update();
        } catch (DatabaseException $error) {
            return $error->getMessage();
        }

        $this->fail('the update must be refused');
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function definitions(Database $database, string $collection = self::COLLECTION): array
    {
        $definitions = [];
        /** @var array<Attribute|Document> $attributes */
        $attributes = $database->getCollection($collection)->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            $document = $attribute instanceof Attribute ? $attribute->toDocument() : $attribute;
            $definitions[$document->getId()] = $document->getArrayCopy();
        }

        return $definitions;
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('attributes')->setNamespace('update_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'name', size: 64, required: true),
                Attribute::string(key: 'label', size: 64),
                Attribute::string(key: 'tags', size: 16, array: true),
                Attribute::integer(key: 'count'),
                Attribute::float(key: 'ratio'),
                Attribute::boolean(key: 'flag'),
                Attribute::datetime(key: 'occurredAt'),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        return $database;
    }
}
