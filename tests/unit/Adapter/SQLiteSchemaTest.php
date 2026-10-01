<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class SQLiteSchemaTest extends TestCase
{
    private const string NAMESPACE = 'schema';

    private PDO $pdo;

    private SQLite $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->adapter = new SQLite($this->pdo);
        $this->database = new Database($this->adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: 'notes',
            attributes: [Attribute::string('title', size: 64)],
            indexes: [Index::key(key: 'title_index', attributes: ['title'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
    }

    public function testRenamingAnIndexTheMetadataLacksReturnsFalse(): void
    {
        $this->assertFalse($this->adapter->renameIndex('notes', 'missing', 'renamed'));

        $this->assertSame([self::NAMESPACE . '__notes_title_index'], $this->indexNames());
    }

    public function testRenamingAnIndexOfAnUnknownCollectionIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $this->adapter->renameIndex('missing', 'title_index', 'renamed');
    }

    public function testRenamingAStoredIndexRenamesIt(): void
    {
        $this->assertTrue($this->adapter->renameIndex('notes', 'title_index', 'renamed'));

        $this->assertSame([self::NAMESPACE . '__notes_renamed'], $this->indexNames());
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function stringSizes(): iterable
    {
        yield 'longest varchar' => [16381, 'VARCHAR(16381)'];
        yield 'above the varchar maximum' => [16382, 'TEXT'];
        yield 'above text' => [65536, 'MEDIUMTEXT'];
        yield 'above medium text' => [16777216, 'LONGTEXT'];
    }

    #[DataProvider('stringSizes')]
    public function testAStringColumnGrowsIntoTheTypeItsSizeNeeds(int $size, string $type): void
    {
        $this->assertTrue($this->adapter->createCollection('texts', [Attribute::string('body', size: $size)]));

        $this->assertSame($type, $this->columnType('texts', 'body'));
    }

    public function testALongStringIsStoredWhole(): void
    {
        $this->database->createCollection(new Collection(
            id: 'articles',
            attributes: [Attribute::string('body', size: 20000000)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $value = \str_repeat('abc', 10000);

        $this->database->createDocument('articles', new Document(['$id' => 'long', 'body' => $value]));

        $this->assertSame('LONGTEXT', $this->columnType('articles', 'body'));
        $this->assertSame($value, $this->database->getDocument('articles', 'long')->getAttribute('body'));
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function invalidVarcharSizes(): iterable
    {
        yield 'zero' => [0, 'VARCHAR size 0 is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
        yield 'negative' => [-1, 'VARCHAR size -1 is invalid; must be > 0. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
        yield 'above the maximum' => [16382, 'VARCHAR size 16382 exceeds maximum varchar length 16381. Use TEXT, MEDIUMTEXT, or LONGTEXT instead.'];
    }

    #[DataProvider('invalidVarcharSizes')]
    public function testAVarcharColumnOutsideItsSizesIsRefused(int $size, string $message): void
    {
        try {
            $this->adapter->createCollection('codes', [Attribute::varchar('code', size: $size)]);
            $this->fail('A varchar column outside its sizes must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame($message, $error->getMessage());
        }

        $this->assertFalse($this->adapter->exists(self::NAMESPACE, 'codes'));
    }

    public function testAVarcharColumnWithinItsSizesIsCreated(): void
    {
        $this->assertTrue($this->adapter->createCollection('codes', [Attribute::varchar('code', size: 16381)]));

        $this->assertSame('VARCHAR(16381)', $this->columnType('codes', 'code'));
    }

    public function testASpatialColumnIsUntypedAndKeepsItsWktAsText(): void
    {
        $this->assertTrue($this->adapter->createCollection('places', [Attribute::point('position')]));
        $this->assertSame('', $this->columnType('places', 'position'));

        $collection = new Document([
            '$id' => 'places',
            'attributes' => [
                new Document(['$id' => 'position', 'key' => 'position', 'type' => ColumnType::Point->value]),
            ],
        ]);
        $this->adapter->createDocuments($collection, [
            new Document(['$id' => 'origin', '$permissions' => [], 'position' => 'POINT(1 2)']),
        ]);

        $statement = $this->pdo->query('SELECT `position` FROM `' . self::NAMESPACE . "_places` WHERE `_uid` = 'origin'");
        $this->assertInstanceOf(\PDOStatement::class, $statement);
        $this->assertSame('POINT(1 2)', $statement->fetchColumn());
    }

    /**
     * @return list<string>
     */
    private function indexNames(): array
    {
        $statement = $this->pdo->query("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL ORDER BY name");
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        $names = [];
        foreach ($statement->fetchAll(PDO::FETCH_COLUMN) as $name) {
            if (\is_string($name) && (\str_ends_with($name, '_title_index') || \str_ends_with($name, '_renamed'))) {
                $names[] = $name;
            }
        }

        return $names;
    }

    private function columnType(string $table, string $column): ?string
    {
        $statement = $this->pdo->query('PRAGMA table_info(`' . self::NAMESPACE . '_' . $table . '`)');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
            if (\is_array($row) && ($row['name'] ?? null) === $column) {
                return \is_string($row['type'] ?? null) ? $row['type'] : null;
            }
        }

        return null;
    }
}
