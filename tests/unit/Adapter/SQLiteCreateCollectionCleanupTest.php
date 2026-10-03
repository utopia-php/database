<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Index;

final class SQLiteCreateCollectionCleanupTest extends TestCase
{
    private const string NAMESPACE = 'cleanup';

    private PDO $pdo;

    private SQLite $adapter;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->adapter = new SQLite($this->pdo);
        $this->adapter->setDatabase('main');
        $this->adapter->setNamespace(self::NAMESPACE);
    }

    public function testADeclaredIndexThatFailsLeavesNoTableAndTheCollectionCanBeCreatedAgain(): void
    {
        $failure = null;
        try {
            $this->adapter->createCollection('books', [Attribute::string('title', size: 64)], [
                Index::key(key: 'missing_index', attributes: ['missing']),
            ]);
        } catch (Throwable $error) {
            $failure = $error;
        }

        $this->assertNotNull($failure, 'A declared index on a missing column must fail the collection');
        $this->assertInstanceOf(NotFoundException::class, $failure);
        $this->assertSame('Attribute not found', $failure->getMessage());
        $previous = $failure->getPrevious();
        $this->assertInstanceOf(PDOException::class, $previous);
        $this->assertStringContainsString('missing', $previous->getMessage());
        $this->assertSame([], $this->tables());

        $this->assertTrue($this->adapter->createCollection('books', [Attribute::string('title', size: 64)], [
            Index::key(key: 'title_index', attributes: ['title']),
        ]));
        $this->assertSame([self::NAMESPACE . '_books', self::NAMESPACE . '_books_perms'], $this->tables());
    }

    public function testACollectionThatAlreadyExistsKeepsItsTables(): void
    {
        $this->adapter->createCollection('books', [Attribute::string('title', size: 64)]);

        try {
            $this->adapter->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('An existing collection must be reported');
        } catch (DuplicateException $error) {
            $this->assertSame('Collection already exists', $error->getMessage());
        }

        $this->assertSame([self::NAMESPACE . '_books', self::NAMESPACE . '_books_perms'], $this->tables());
    }

    /**
     * @return list<string>
     */
    private function tables(): array
    {
        $statement = $this->pdo->query("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '" . self::NAMESPACE . "\\_%' ESCAPE '\\' ORDER BY name");
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        $names = [];
        foreach ($statement->fetchAll(PDO::FETCH_COLUMN) as $name) {
            if (\is_string($name)) {
                $names[] = $name;
            }
        }

        return $names;
    }
}
