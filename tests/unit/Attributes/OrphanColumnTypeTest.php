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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class OrphanColumnTypeTest extends TestCase
{
    private const string COLLECTION = 'items';

    private const string KEY = 'value';

    private const string DOCUMENT = 'row';

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
        $this->assertRefused(fn () => $adapter->getColumnType($type->value, 0));
    }

    #[DataProvider('unstorableTypes')]
    public function testCreateAttributeOverAMatchingOrphanColumnRefusesTheType(ColumnType $type): void
    {
        [$database, $adapter] = $this->database();
        $adapter->createAttribute(self::COLLECTION, Attribute::bigInteger(key: self::KEY));

        $this->assertRefused(fn () => $database->createAttribute(self::COLLECTION, new Attribute(key: self::KEY, type: $type)));

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes);
    }

    #[DataProvider('unstorableTypes')]
    public function testCreateAttributesOverAMatchingOrphanColumnRefusesTheType(ColumnType $type): void
    {
        [$database, $adapter] = $this->database();
        $adapter->createAttribute(self::COLLECTION, Attribute::bigInteger(key: self::KEY));

        $this->assertRefused(fn () => $database->createAttributes(self::COLLECTION, [new Attribute(key: self::KEY, type: $type)]));

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes);
    }

    #[DataProvider('unstorableTypes')]
    public function testRefusalLeavesAnOrphanColumnOfAnotherTypeInPlace(ColumnType $type): void
    {
        [$database, $adapter] = $this->database();
        $adapter->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64));
        $orphan = $this->schemaColumnType($database);

        $this->assertRefused(fn () => $database->createAttribute(self::COLLECTION, new Attribute(key: self::KEY, type: $type)));
        $this->assertSame($orphan, $this->schemaColumnType($database));

        $this->assertRefused(fn () => $database->createAttributes(self::COLLECTION, [new Attribute(key: self::KEY, type: $type)]));
        $this->assertSame($orphan, $this->schemaColumnType($database));

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes);
    }

    public function testCreateAttributeReusesAnOrphanColumnOfTheSameType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::string(key: self::KEY, size: 64));
        $orphan = $this->schemaColumnType($database);

        $this->assertTrue($database->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64)));

        $this->assertSame($orphan, $this->schemaColumnType($database));
        $this->assertSame([self::KEY], $this->keys($database));
        $this->assertSame('kept', $database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
    }

    public function testCreateAttributeReplacesAnOrphanColumnOfAnotherType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::integer(key: self::KEY));
        $orphan = $this->schemaColumnType($database);

        $this->assertTrue($database->createAttribute(self::COLLECTION, Attribute::string(key: self::KEY, size: 64)));

        $this->assertNotSame($orphan, $this->schemaColumnType($database));
        $this->assertSame(\strtolower($adapter->getColumnType(ColumnType::String->value, 64)), $this->schemaColumnType($database));
        $this->assertSame([self::KEY], $this->keys($database));
        $document = $database->getDocument(self::COLLECTION, self::DOCUMENT);
        $this->assertSame(self::DOCUMENT, $document->getId());
        $this->assertNull($document->getAttribute(self::KEY));
    }

    public function testCreateAttributesReusesAnOrphanColumnOfTheSameType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::string(key: self::KEY, size: 64));
        $orphan = $this->schemaColumnType($database);

        $this->assertTrue($database->createAttributes(self::COLLECTION, [
            Attribute::string(key: self::KEY, size: 64),
            Attribute::integer(key: 'count'),
        ]));

        $this->assertSame($orphan, $this->schemaColumnType($database));
        $this->assertSame([self::KEY, 'count'], $this->keys($database));
        $this->assertSame('kept', $database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute(self::KEY));
    }

    public function testCreateAttributesReplacesAnOrphanColumnOfAnotherType(): void
    {
        [$database, $adapter] = $this->databaseWithAnOrphanValue(Attribute::integer(key: self::KEY));
        $orphan = $this->schemaColumnType($database);

        $this->assertTrue($database->createAttributes(self::COLLECTION, [
            Attribute::string(key: self::KEY, size: 64),
            Attribute::integer(key: 'count'),
        ]));

        $this->assertNotSame($orphan, $this->schemaColumnType($database));
        $this->assertSame(\strtolower($adapter->getColumnType(ColumnType::String->value, 64)), $this->schemaColumnType($database));
        $this->assertSame([self::KEY, 'count'], $this->keys($database));
        $document = $database->getDocument(self::COLLECTION, self::DOCUMENT);
        $this->assertSame(self::DOCUMENT, $document->getId());
        $this->assertNull($document->getAttribute(self::KEY));
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
            \array_values($database->getCollection(self::COLLECTION)->attributes),
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
        $database->createCollection(new Collection(id: self::COLLECTION));

        return [$database, $adapter];
    }

    private function schemaColumnType(Database $database): string
    {
        foreach ($database->getSchemaAttributes(self::COLLECTION) as $column) {
            if ($column->getId() === self::KEY) {
                $columnType = $column->getAttribute('columnType');
                $this->assertIsString($columnType);

                return $columnType;
            }
        }

        $this->fail('The orphan column is no longer in the schema');
    }

    private function assertRefused(callable $operation): void
    {
        try {
            $operation();
        } catch (DatabaseException $error) {
            $this->assertSame(DatabaseException::class, $error::class, $error->getMessage());

            return;
        }

        $this->fail('Expected the unstorable type to be refused');
    }
}
