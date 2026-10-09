<?php

namespace Tests\Unit\Attributes;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Schema\Column;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class OrphanColumnTypeTest extends TestCase
{
    private const string COLLECTION = 'items';

    private const string KEY = 'value';

    private const string DOCUMENT = 'row';

    private const string UNVALIDATED = 'unvalidated';

    private const int OVERSIZED_NAME_LENGTH = 257;

    private PDO $pdo;

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
     * @return array<string, array{ColumnType}>
     */
    public static function unstorableTypes(): array
    {
        $cases = [];
        foreach (self::UNSTORABLE as $type) {
            $cases[$type->value] = [$type];
        }

        return $cases;
    }

    /**
     * @return array<string, array{SQL, ColumnType}>
     */
    public static function sqlAdaptersAndUnstorableTypes(): array
    {
        $adapters = [
            'MariaDB' => new MariaDB(new stdClass()),
            'Postgres' => new Postgres(new stdClass()),
            'SQLite' => new SQLite(new PDO('sqlite::memory:')),
        ];

        $cases = [];
        foreach ($adapters as $name => $adapter) {
            foreach (self::UNSTORABLE as $type) {
                $cases[$type->value.' on '.$name] = [$adapter, $type];
            }
        }

        return $cases;
    }

    #[DataProvider('sqlAdaptersAndUnstorableTypes')]
    public function testNoSqlAdapterMapsAnUnstorableTypeToAColumn(SQL $adapter, ColumnType $type): void
    {
        $this->assertRefused(fn () => $adapter->getColumnType(Attribute::fromArray(['key' => self::KEY, 'type' => $type])), StructureException::class);
    }

    #[DataProvider('unstorableTypes')]
    public function testCreateAttributeOverAMatchingOrphanColumnRefusesTheType(ColumnType $type): void
    {
        [$database, $adapter] = $this->database();
        $adapter->createAttribute(self::COLLECTION, Attribute::bigInteger(key: self::KEY));

        $this->assertRefused(fn () => $database->createAttribute(self::COLLECTION, Attribute::fromArray(['key' => self::KEY, 'type' => $type])), StructureException::class);

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes());
    }

    #[DataProvider('unstorableTypes')]
    public function testCreateAttributesOverAMatchingOrphanColumnRefusesTheType(ColumnType $type): void
    {
        [$database, $adapter] = $this->database();
        $adapter->createAttribute(self::COLLECTION, Attribute::bigInteger(key: self::KEY));

        $this->assertRefused(fn () => $database->createAttributes(self::COLLECTION, [Attribute::fromArray(['key' => self::KEY, 'type' => $type])]), StructureException::class);

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes());
    }

    #[DataProvider('unstorableTypes')]
    public function testRefusalLeavesAnOrphanColumnOfAnotherTypeInPlace(ColumnType $type): void
    {
        [$database, $adapter] = $this->database();
        $adapter->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64));
        $orphan = $this->schemaColumnType($database);

        $this->assertRefused(fn () => $database->createAttribute(self::COLLECTION, Attribute::fromArray(['key' => self::KEY, 'type' => $type])), StructureException::class);
        $this->assertSame($orphan, $this->schemaColumnType($database));

        $this->assertRefused(fn () => $database->createAttributes(self::COLLECTION, [Attribute::fromArray(['key' => self::KEY, 'type' => $type])]), StructureException::class);
        $this->assertSame($orphan, $this->schemaColumnType($database));

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes());
    }

    public function testCreateAttributeReusesAnOrphanColumnOfTheSameType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::string(key: self::KEY, size: 64));
        $orphan = $this->schemaColumnType($database);

        $this->assertSame(self::KEY, $database->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64))->key);

        $this->assertSame($orphan, $this->schemaColumnType($database));
        $this->assertSame([self::KEY], $this->keys($database));
        $this->assertSame('kept', $database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
    }

    public function testCreateAttributeReplacesAnOrphanColumnOfAnotherType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::integer(key: self::KEY));
        $orphan = $this->schemaColumnType($database);

        $this->assertSame(self::KEY, $database->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64))->key);

        $this->assertNotSame($orphan, $this->schemaColumnType($database));
        $this->assertSame($adapter->getColumnType(Attribute::string(key: self::KEY, size: 64)), $this->schemaColumnType($database));
        $this->assertSame([self::KEY], $this->keys($database));
        $document = $database->getDocument(self::COLLECTION, self::DOCUMENT);
        $this->assertSame(self::DOCUMENT, $document->getId());
        $this->assertNull($document->getAttribute(self::KEY));
    }

    public function testCreateAttributesReusesAnOrphanColumnOfTheSameType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::string(key: self::KEY, size: 64));
        $orphan = $this->schemaColumnType($database);

        $created = $database->createAttributes(self::COLLECTION, [
            Attribute::string(key: self::KEY, size: 64),
            Attribute::integer(key: 'count'),
        ]);

        $this->assertSame([self::KEY, 'count'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $created));

        $this->assertSame($orphan, $this->schemaColumnType($database));
        $this->assertSame([self::KEY, 'count'], $this->keys($database));
        $this->assertSame('kept', $database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
    }

    public function testCreateAttributesReplacesAnOrphanColumnOfAnotherType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::integer(key: self::KEY));
        $orphan = $this->schemaColumnType($database);

        $created = $database->createAttributes(self::COLLECTION, [
            Attribute::string(key: self::KEY, size: 64),
            Attribute::integer(key: 'count'),
        ]);

        $this->assertSame([self::KEY, 'count'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $created));

        $this->assertNotSame($orphan, $this->schemaColumnType($database));
        $this->assertSame($adapter->getColumnType(Attribute::string(key: self::KEY, size: 64)), $this->schemaColumnType($database));
        $this->assertSame([self::KEY, 'count'], $this->keys($database));
        $document = $database->getDocument(self::COLLECTION, self::DOCUMENT);
        $this->assertSame(self::DOCUMENT, $document->getId());
        $this->assertNull($document->getAttribute(self::KEY));
    }

    public function testRenameAdoptsAnExistingSchemaOnlyTarget(): void
    {
        [$database, $adapter] = $this->database();
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'before', size: 64));
        $adapter->renameAttribute(self::COLLECTION, 'before', 'after');

        $database->renameAttribute(self::COLLECTION, 'before', 'after');

        $this->assertSame(['after'], $this->keys($database));
    }

    public function testCreateAttributesRollbackDropsTheBatch(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->unvalidatedDatabase($adapter);

        try {
            $database->createAttributes(self::UNVALIDATED, [
                Attribute::integer(key: 'age'),
                Attribute::string(key: 'nick', size: 16),
            ]);
            $this->fail('The metadata write of an oversized collection name must fail');
        } catch (DatabaseException $error) {
            $this->assertStringStartsWith('Failed to persist metadata after retries for attributes creation: ', $error->getMessage());
            $this->assertInstanceOf(StructureException::class, $error->getPrevious());
        }

        $this->assertSame([], \array_intersect(['age', 'nick'], $this->columns($database, self::UNVALIDATED)));
        $this->assertSame([], $database->getCollection(self::UNVALIDATED)->attributes());
    }

    public function testCreateAttributesRollbackCollectsCleanupErrors(): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            #[\Override]
            public function deleteAttribute(string $collection, string $id): bool
            {
                if ($id === 'nick') {
                    throw new DatabaseException('Column is locked');
                }

                return parent::deleteAttribute($collection, $id);
            }
        };
        $database = $this->unvalidatedDatabase($adapter);

        try {
            $database->createAttributes(self::UNVALIDATED, [
                Attribute::integer(key: 'age'),
                Attribute::string(key: 'nick', size: 16),
            ]);
            $this->fail('The metadata write of an oversized collection name must fail');
        } catch (DatabaseException $error) {
            $this->assertStringStartsWith('Failed to persist metadata after retries and cleanup encountered errors for attributes creation: ', $error->getMessage());
            $this->assertStringEndsWith(' | Cleanup errors: Column is locked', $error->getMessage());
            $this->assertInstanceOf(StructureException::class, $error->getPrevious());
        }

        $columns = $this->columns($database, self::UNVALIDATED);
        $this->assertNotContains('age', $columns);
        $this->assertContains('nick', $columns);
    }

    private function unvalidatedDatabase(SQLite $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('orphan_column_type')
            ->setNamespace('orphan_column_type_'.\uniqid());
        $database->create();
        $collection = Collection::create(id: self::UNVALIDATED, name: \str_repeat('n', self::OVERSIZED_NAME_LENGTH));
        $database->skipValidation(fn (): Collection => $database->createCollection($collection));

        return $database;
    }

    /**
     * @return array<string>
     */
    private function columns(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Column $column): string => $column->name,
            $database->getSchemaAttributes($collection),
        );
    }

    /**
     * @return array{Database, SQLite}
     */
    private function databaseWithAnOrphanValue(Attribute $orphan): array
    {
        [$database, $adapter] = $this->database();
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->createDocument(self::COLLECTION, new Document([
            Document::ID => self::DOCUMENT,
            Document::PERMISSIONS => [Permission::read(Role::any())],
        ]));
        $adapter->createAttribute(self::COLLECTION, $orphan);
        $this->pdo->exec('UPDATE `'.$database->getNamespace().'_'.self::COLLECTION.'` SET `'.self::KEY.'` = \'kept\'');

        return [$database, $adapter];
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes(),
        );
    }

    /**
     * @return array{Database, SQLite}
     */
    private function database(): array
    {
        $this->pdo = new PDO('sqlite::memory:');
        $adapter = new SQLite($this->pdo);
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('orphan_column_type')
            ->setNamespace('orphan_column_type_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: self::COLLECTION));

        return [$database, $adapter];
    }

    private function schemaColumnType(Database $database): string
    {
        foreach ($database->getSchemaAttributes(self::COLLECTION) as $column) {
            if ($column->name === self::KEY) {
                return $column->type;
            }
        }

        $this->fail('The orphan column is no longer in the schema');
    }

    /**
     * @param  class-string<DatabaseException>  $expected
     */
    private function assertRefused(callable $operation, string $expected = DatabaseException::class): void
    {
        try {
            $operation();
        } catch (DatabaseException $error) {
            $this->assertSame($expected, $error::class, $error->getMessage());

            return;
        }

        $this->fail('Expected the unstorable type to be refused');
    }
}
