<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\Redis as RedisAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\PDO;
use Utopia\Database\Query;

class PostgresTest extends Base
{
    public static ?Database $database = null;
    protected static ?PDO $pdo = null;
    protected static string $namespace;

    /**
     * @reture Adapter
     */
    public static function getDatabase(): Database
    {
        if (!is_null(self::$database)) {
            return self::$database;
        }

        $dbHost = 'postgres';
        $dbPort = '5432';
        $dbUser = 'root';
        $dbPass = 'password';

        $pdo = new PDO("pgsql:host={$dbHost};port={$dbPort};", $dbUser, $dbPass, Postgres::getPDOAttributes());
        $redis = new Redis();
        $redis->connect('redis', 6379);
        $redis->flushAll();
        $cache = new Cache(new RedisAdapter($redis));

        $database = new Database(new Postgres($pdo), $cache);
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace(static::$namespace = 'myapp_' . uniqid());

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        self::$pdo = $pdo;
        return self::$database = $database;
    }

    protected static function deleteColumn(string $collection, string $column): bool
    {
        $sqlTable = '"' . self::getDatabase()->getDatabase() . '"."' . self::getDatabase()->getNamespace() . '_' . $collection . '"';
        $sql = "ALTER TABLE {$sqlTable} DROP COLUMN \"{$column}\"";

        self::$pdo->exec($sql);

        return true;
    }

    protected static function deleteIndex(string $collection, string $index): bool
    {
        $key = "\"".self::getDatabase()->getNamespace()."_".self::getDatabase()->getTenant()."_{$collection}_{$index}\"";

        $sql = "DROP INDEX \"".self::getDatabase()->getDatabase()."\".{$key}";

        self::$pdo->exec($sql);

        return true;
    }

    public function testVectorAttributes(): void
    {
        $database = static::getDatabase();

        // Test that vector attributes can only be created on PostgreSQL
        $this->assertEquals(true, $database->createCollection('vectorCollection'));

        // Create a vector attribute with 3 dimensions
        $this->assertEquals(true, $database->createAttribute('vectorCollection', 'embedding', Database::VAR_VECTOR, 3, true));

        // Create a vector attribute with 128 dimensions
        $this->assertEquals(true, $database->createAttribute('vectorCollection', 'large_embedding', Database::VAR_VECTOR, 128, false, null));

        // Verify the attributes were created
        $collection = $database->getCollection('vectorCollection');
        $attributes = $collection->getAttribute('attributes');
        
        $embeddingAttr = null;
        $largeEmbeddingAttr = null;
        
        foreach ($attributes as $attr) {
            if ($attr['key'] === 'embedding') {
                $embeddingAttr = $attr;
            } elseif ($attr['key'] === 'large_embedding') {
                $largeEmbeddingAttr = $attr;
            }
        }

        $this->assertNotNull($embeddingAttr);
        $this->assertNotNull($largeEmbeddingAttr);
        $this->assertEquals(Database::VAR_VECTOR, $embeddingAttr['type']);
        $this->assertEquals(3, $embeddingAttr['size']);
        $this->assertEquals(Database::VAR_VECTOR, $largeEmbeddingAttr['type']);
        $this->assertEquals(128, $largeEmbeddingAttr['size']);
    }

    public function testVectorInvalidDimensions(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('vectorErrorCollection'));

        // Test invalid dimensions
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions must be a positive integer');
        $database->createAttribute('vectorErrorCollection', 'bad_embedding', Database::VAR_VECTOR, 0, true);
    }

    public function testVectorTooManyDimensions(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('vectorLimitCollection'));

        // Test too many dimensions (pgvector limit is 16000)
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Vector dimensions cannot exceed 16000');
        $database->createAttribute('vectorLimitCollection', 'huge_embedding', Database::VAR_VECTOR, 16001, true);
    }

    public function testVectorDocuments(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('vectorDocuments'));
        $this->assertEquals(true, $database->createAttribute('vectorDocuments', 'name', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, $database->createAttribute('vectorDocuments', 'embedding', Database::VAR_VECTOR, 3, true));

        // Create documents with vector data
        $doc1 = $database->createDocument('vectorDocuments', new Document([
            'name' => 'Document 1',
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        $doc2 = $database->createDocument('vectorDocuments', new Document([
            'name' => 'Document 2', 
            'embedding' => [0.0, 1.0, 0.0]
        ]));

        $doc3 = $database->createDocument('vectorDocuments', new Document([
            'name' => 'Document 3',
            'embedding' => [0.0, 0.0, 1.0]
        ]));

        $this->assertNotEmpty($doc1->getId());
        $this->assertNotEmpty($doc2->getId());
        $this->assertNotEmpty($doc3->getId());

        $this->assertEquals([1.0, 0.0, 0.0], $doc1->getAttribute('embedding'));
        $this->assertEquals([0.0, 1.0, 0.0], $doc2->getAttribute('embedding'));
        $this->assertEquals([0.0, 0.0, 1.0], $doc3->getAttribute('embedding'));
    }

    public function testVectorQueries(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('vectorQueries'));
        $this->assertEquals(true, $database->createAttribute('vectorQueries', 'name', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, $database->createAttribute('vectorQueries', 'embedding', Database::VAR_VECTOR, 3, true));

        // Create test documents
        $database->createDocument('vectorQueries', new Document([
            'name' => 'Test 1',
            'embedding' => [1.0, 0.0, 0.0]
        ]));

        $database->createDocument('vectorQueries', new Document([
            'name' => 'Test 2',
            'embedding' => [0.0, 1.0, 0.0]
        ]));

        $database->createDocument('vectorQueries', new Document([
            'name' => 'Test 3',
            'embedding' => [0.5, 0.5, 0.0]
        ]));

        // Test vector dot product query
        $results = $database->find('vectorQueries', [
            Query::vectorDot('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id')
        ]);

        $this->assertCount(3, $results);

        // Test vector cosine distance query
        $results = $database->find('vectorQueries', [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id')
        ]);

        $this->assertCount(3, $results);

        // Test vector euclidean distance query
        $results = $database->find('vectorQueries', [
            Query::vectorEuclidean('embedding', [1.0, 0.0, 0.0]),
            Query::orderAsc('$id')
        ]);

        $this->assertCount(3, $results);
    }

    public function testVectorQueryValidation(): void
    {
        $database = static::getDatabase();

        $this->assertEquals(true, $database->createCollection('vectorValidation'));
        $this->assertEquals(true, $database->createAttribute('vectorValidation', 'embedding', Database::VAR_VECTOR, 3, true));
        $this->assertEquals(true, $database->createAttribute('vectorValidation', 'name', Database::VAR_STRING, 255, true));

        // Test that vector queries fail on non-vector attributes
        $this->expectException(DatabaseException::class);
        $database->find('vectorValidation', [
            Query::vectorDot('name', [1.0, 0.0, 0.0])
        ]);
    }
}
