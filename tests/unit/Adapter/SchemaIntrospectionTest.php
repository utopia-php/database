<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Schema\Column;
use Utopia\Database\Schema\Index as SchemaIndex;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

final class SchemaIntrospectionTest extends TestCase
{
    private const string COLLECTION = 'people';

    private PDO $pdo;

    public function testSQLiteReadsItsColumnsBackAsTypedColumns(): void
    {
        [$database, $adapter] = $this->sqlite();
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'name', size: 64));
        $database->createAttribute(self::COLLECTION, Attribute::integer(key: 'age'));

        $columns = $this->columns($database);

        $this->assertContainsOnlyInstancesOf(Column::class, $database->getSchemaAttributes(self::COLLECTION));
        $this->assertSame($adapter->getColumnType(Attribute::string(key: 'name', size: 64)), $columns['name']->type);
        $this->assertSame('VARCHAR(64)', $columns['name']->type);
        $this->assertSame(64, $columns['name']->length);
        $this->assertTrue($columns['name']->nullable);
        $this->assertSame($adapter->getColumnType(Attribute::integer(key: 'age')), $columns['age']->type);
        $this->assertNull($columns['age']->length);
        $this->assertFalse($columns[Storage::UID]->nullable);
    }

    public function testSQLiteReadsItsIndexesBackAsTypedIndexes(): void
    {
        [$database] = $this->sqlite();
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'name', size: 64));
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'email', size: 64));
        $database->createIndex(self::COLLECTION, Index::key(key: 'by_name', attributes: ['name']));
        $database->createIndex(self::COLLECTION, Index::unique(key: 'by_email', attributes: ['email']));
        $database->createIndex(self::COLLECTION, Index::fulltext(key: 'search', attributes: ['name']));

        $indexes = [];
        foreach ($database->getSchemaIndexes(self::COLLECTION) as $index) {
            $this->assertInstanceOf(SchemaIndex::class, $index);
            $indexes[$index->name] = $index;
        }

        $this->assertSame([IndexType::Key, ['name'], [null]], [$indexes['by_name']->type, $indexes['by_name']->columns, $indexes['by_name']->lengths]);
        $this->assertSame([IndexType::Unique, ['email']], [$indexes['by_email']->type, $indexes['by_email']->columns]);
        $this->assertSame(IndexType::Fulltext, $indexes['search']->type);
    }

    public function testAnEngineOnlyColumnIsReadBackWithoutBecomingAnAttribute(): void
    {
        [$database] = $this->sqlite();
        $table = '`'.$database->getNamespace().'_'.self::COLLECTION.'`';
        $this->pdo->exec("ALTER TABLE {$table} ADD COLUMN `legacy` TINYINT(4)");
        $this->pdo->exec("ALTER TABLE {$table} ADD COLUMN `settings` JSON");

        $columns = $this->columns($database);

        $this->assertSame('TINYINT', $columns['legacy']->type);
        $this->assertSame('LONGTEXT', $columns['settings']->type);
        foreach ([Storage::SEQUENCE, Storage::UID, Storage::CREATED_AT, Storage::UPDATED_AT, Storage::PERMISSIONS] as $internal) {
            $this->assertArrayHasKey($internal, $columns);
        }
    }

    public function testAnAttributeOverAnEngineOnlyColumnOfAnotherTypeReplacesIt(): void
    {
        [$database, $adapter] = $this->sqlite();
        $this->pdo->exec('ALTER TABLE `'.$database->getNamespace().'_'.self::COLLECTION.'` ADD COLUMN `legacy` TINYINT(4)');

        $created = $database->createAttribute(self::COLLECTION, Attribute::integer(key: 'legacy'));

        $this->assertSame('legacy', $created->key);
        $this->assertSame($adapter->getColumnType(Attribute::integer(key: 'legacy')), $this->columns($database)['legacy']->type);
    }

    /**
     * @return iterable<string, array{class-string<MariaDB>}>
     */
    public static function mariaDBFamily(): iterable
    {
        yield 'MariaDB' => [MariaDB::class];
        yield 'MySQL' => [MySQL::class];
    }

    /**
     * @param  class-string<MariaDB>  $class
     */
    #[DataProvider('mariaDBFamily')]
    public function testMariaDBReadsTheCatalogIntoTypedColumnsAndIndexes(string $class): void
    {
        $adapter = new $class($this->catalog([
            'INFORMATION_SCHEMA.COLUMNS' => [
                ['name' => '_tenant', 'type' => 'int(11) unsigned', 'length' => null, 'nullable' => 'YES'],
                ['name' => 'name', 'type' => 'varchar(64)', 'length' => '64', 'nullable' => 'YES'],
                ['name' => 'tags', 'type' => 'json', 'length' => null, 'nullable' => 'YES'],
                ['name' => 'spot', 'type' => 'point not null', 'length' => null, 'nullable' => 'NO'],
            ],
            'INFORMATION_SCHEMA.STATISTICS' => [
                ['name' => 'PRIMARY', 'columnName' => '_id', 'nonUnique' => '0', 'indexType' => 'BTREE', 'subPart' => null],
                ['name' => 'by_name', 'columnName' => '_tenant', 'nonUnique' => '1', 'indexType' => 'BTREE', 'subPart' => null],
                ['name' => 'by_name', 'columnName' => 'name', 'nonUnique' => '1', 'indexType' => 'BTREE', 'subPart' => '16'],
                ['name' => 'search', 'columnName' => 'name', 'nonUnique' => '1', 'indexType' => 'FULLTEXT', 'subPart' => null],
                ['name' => 'area', 'columnName' => 'spot', 'nonUnique' => '1', 'indexType' => 'SPATIAL', 'subPart' => null],
            ],
        ]));

        $this->assertTrue($adapter->supports(Capability::SchemaIntrospection));
        $this->assertEquals([
            new Column('_tenant', 'INT UNSIGNED', null, true),
            new Column('name', 'VARCHAR(64)', 64, true),
            new Column('tags', 'LONGTEXT', null, true),
            new Column('spot', 'POINT', null, false),
        ], $adapter->getSchemaAttributes(self::COLLECTION));
        $this->assertSame('VARCHAR(64)', $adapter->getColumnType(Attribute::string(key: 'name', size: 64)));
        $this->assertSame('LONGTEXT', $adapter->getColumnType(Attribute::string(key: 'tags', size: 64, array: true)));
        $this->assertEquals([
            new SchemaIndex('PRIMARY', IndexType::Unique, ['_id'], [null]),
            new SchemaIndex('by_name', IndexType::Key, ['_tenant', 'name'], [null, 16]),
            new SchemaIndex('search', IndexType::Fulltext, ['name'], [null]),
            new SchemaIndex('area', IndexType::Spatial, ['spot'], [null]),
        ], $adapter->getSchemaIndexes(self::COLLECTION));
    }

    public function testPostgresReadsTheCatalogIntoTypedColumnsAndIndexes(): void
    {
        $adapter = new Postgres($this->catalog([
            'pg_catalog.pg_attribute' => [
                ['name' => '_id', 'type' => 'bigint', 'length' => null, 'nullable' => false],
                ['name' => 'name', 'type' => 'character varying(64)', 'length' => 64, 'nullable' => true],
                ['name' => 'born', 'type' => 'timestamp(3) without time zone', 'length' => null, 'nullable' => true],
                ['name' => 'spot', 'type' => 'geometry(Point,4326)', 'length' => null, 'nullable' => 't'],
            ],
            'pg_catalog.pg_index' => [
                ['name' => 'namespace_people_pkey', 'unique' => true, 'method' => 'btree', 'operator' => 'int8_ops', 'column' => '_id'],
                ['name' => 'namespace_2_people_by_name', 'unique' => false, 'method' => 'btree', 'operator' => 'text_ops', 'column' => '_tenant'],
                ['name' => 'namespace_2_people_by_name', 'unique' => false, 'method' => 'btree', 'operator' => 'text_ops', 'column' => '"fullName"'],
                ['name' => 'namespace_3_people_by_name', 'unique' => true, 'method' => 'btree', 'operator' => 'text_ops', 'column' => 'name'],
                ['name' => 'namespace_2_people_area', 'unique' => false, 'method' => 'gist', 'operator' => 'gist_geometry_ops_2d', 'column' => 'spot'],
                ['name' => 'namespace_2_people_names', 'unique' => false, 'method' => 'gin', 'operator' => 'gin_trgm_ops', 'column' => 'name'],
            ],
        ]));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        $this->assertTrue($adapter->supports(Capability::SchemaIntrospection));
        $this->assertEquals([
            new Column('_id', 'BIGINT', null, false),
            new Column('name', 'VARCHAR(64)', 64, true),
            new Column('born', 'TIMESTAMP(3)', null, true),
            new Column('spot', 'GEOMETRY(POINT,4326)', null, true),
        ], $adapter->getSchemaAttributes(self::COLLECTION));
        $this->assertSame('VARCHAR(64)', $adapter->getColumnType(Attribute::string(key: 'name', size: 64)));
        $this->assertSame('TIMESTAMP(3)', $adapter->getColumnType(Attribute::datetime(key: 'born')));
        $this->assertSame('GEOMETRY(POINT,4326)', $adapter->getColumnType(Attribute::point(key: 'spot')));
        $this->assertEquals([
            new SchemaIndex('namespace_people_pkey', IndexType::Unique, ['_id'], [null]),
            new SchemaIndex('by_name', IndexType::Key, ['_tenant', 'fullName'], [null, null]),
            new SchemaIndex('namespace_3_people_by_name', IndexType::Unique, ['name'], [null]),
            new SchemaIndex('area', IndexType::Spatial, ['spot'], [null]),
            new SchemaIndex('names', IndexType::Trigram, ['name'], [null]),
        ], $adapter->getSchemaIndexes(self::COLLECTION));
    }

    /**
     * @return iterable<string, array{Adapter}>
     */
    public static function adaptersWithoutIntrospection(): iterable
    {
        yield 'Memory' => [new Memory()];
        yield 'MongoDB' => [new class () extends Mongo {
            public function __construct()
            {
            }
        }];
        yield 'Redis' => [new class () extends Redis {
            public function __construct()
            {
            }
        }];
    }

    #[DataProvider('adaptersWithoutIntrospection')]
    public function testAnAdapterThatCannotIntrospectReportsNothingAndDoesNotDeclareIt(Adapter $adapter): void
    {
        $this->assertFalse($adapter->supports(Capability::SchemaIntrospection));
        $this->assertSame([], $adapter->getSchemaAttributes(self::COLLECTION));
        $this->assertSame([], $adapter->getSchemaIndexes(self::COLLECTION));
        $this->assertNull($adapter->getColumnType(Attribute::string(key: 'name', size: 64)));
    }

    public function testNoReconciliationRunsWithoutIntrospection(): void
    {
        $database = $this->database(new Memory());
        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'name', size: 64));

        $this->assertSame([], $database->getSchemaAttributes(self::COLLECTION));
        $this->assertSame([], $database->getSchemaIndexes(self::COLLECTION));
        $this->expectException(DuplicateException::class);

        $database->createAttribute(self::COLLECTION, Attribute::string(key: 'name', size: 64));
    }

    /**
     * @return array{Database, SQLite}
     */
    private function sqlite(): array
    {
        $this->pdo = new PDO('sqlite::memory:');
        $adapter = new SQLite($this->pdo);

        return [$this->database($adapter), $adapter];
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('introspection')
            ->setNamespace('introspection_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        return $database;
    }

    /**
     * @return array<string, Column>
     */
    private function columns(Database $database): array
    {
        $columns = [];
        foreach ($database->getSchemaAttributes(self::COLLECTION) as $column) {
            $columns[$column->name] = $column;
        }

        return $columns;
    }

    /**
     * A driver whose statements answer a catalog read with the rows given for the catalog it names.
     *
     * @param  array<string, list<array<string, mixed>>>  $catalogs
     */
    private function catalog(array $catalogs): PDO
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($catalogs): PDOStatement {
            $rows = [];
            foreach ($catalogs as $catalog => $answer) {
                if (\str_contains($query, $catalog)) {
                    $rows = $answer;
                }
            }

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('fetchAll')->willReturn($rows);

            return $statement;
        });

        return $pdo;
    }
}
