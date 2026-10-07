<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

final class PostgresSchemaTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    public function testCreateCollectionAddsColumnsOnlyForRelationshipSidesThatStoreAKey(): void
    {
        $this->adapter()->createCollection('books', [
            Attribute::string('title', size: 64),
            $this->relationship('tags', RelationshipType::ManyToMany, twoWay: true, side: RelationshipSide::Parent),
            $this->relationship('cover', RelationshipType::OneToOne, twoWay: false, side: RelationshipSide::Child),
            $this->relationship('chapters', RelationshipType::OneToMany, twoWay: true, side: RelationshipSide::Parent),
            $this->relationship('shelf', RelationshipType::ManyToOne, twoWay: true, side: RelationshipSide::Child),
            $this->relationship('isbn', RelationshipType::OneToOne, twoWay: false, side: RelationshipSide::Parent),
            $this->relationship('summary', RelationshipType::OneToOne, twoWay: true, side: RelationshipSide::Child),
            $this->relationship('series', RelationshipType::OneToMany, twoWay: true, side: RelationshipSide::Child),
            $this->relationship('publisher', RelationshipType::ManyToOne, twoWay: true, side: RelationshipSide::Parent),
        ]);

        $create = $this->statements[0] ?? '';
        $this->assertStringStartsWith('CREATE TABLE "database"."namespace_books"', $create);

        foreach (['title', 'isbn', 'summary', 'series', 'publisher'] as $stored) {
            $this->assertStringContainsString('"' . $stored . '" ', $create, $stored . ' stores a column');
        }

        foreach (['tags', 'cover', 'chapters', 'shelf'] as $skipped) {
            $this->assertStringNotContainsString('"' . $skipped . '"', $create, $skipped . ' stores nothing on this side');
        }
    }

    /**
     * @return iterable<string, array{IndexType}>
     */
    public static function unknownIndexTypes(): iterable
    {
        yield 'ttl' => [IndexType::Ttl];
    }

    #[DataProvider('unknownIndexTypes')]
    public function testCreateIndexRefusesATypeTheEngineDoesNotCreate(IndexType $type): void
    {
        try {
            $this->adapter()->createIndex('events', Index::fromArray(['key' => 'happened_index', 'type' => $type, 'attributes' => ['happened'], 'ttl' => 3600]));
            $this->fail('An index type the engine does not create must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame(
                'Unknown index type: ' . $type->value . '. Must be one of key, unique, fulltext, spatial, object, hnsw_euclidean, hnsw_cosine, hnsw_dot',
                $error->getMessage(),
            );
        }

        $this->assertSame([], $this->statements);
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function invalidPathSegments(): iterable
    {
        yield 'space' => ['meta.bad key', 'bad key'];
        yield 'quote' => ["meta.it's", "it's"];
        yield 'empty' => ['meta..leaf', ''];
        yield 'nested' => ['meta.inner.bad;drop', 'bad;drop'];
    }

    #[DataProvider('invalidPathSegments')]
    public function testANestedObjectIndexPathWithAnInvalidSegmentIsRefused(string $path, string $segment): void
    {
        try {
            $this->adapter()->createIndex('books', Index::object('meta_index', $path), [$path => ColumnType::Object->value]);
            $this->fail('A nested index path with an invalid segment must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Invalid JSON key ' . $segment, $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    public function testANestedObjectIndexPathIsIndexedAsText(): void
    {
        $this->adapter()->createIndex('books', Index::object('meta_index', 'meta.inner.leaf-key'), ['meta.inner.leaf-key' => ColumnType::Object->value]);

        $this->assertSame(
            ['CREATE INDEX "namespace__books_meta_index" ON "database"."namespace_books" USING GIN ((("meta"->\'inner\'->>\'leaf-key\')::text))'],
            $this->statements,
        );
    }

    public function testUpdatingAnArrayAttributeKeepsItsColumnJsonb(): void
    {
        $this->adapter()->updateAttribute('books', 'tags', Attribute::string('tags', size: 64, array: true));

        $this->assertSame('ALTER TABLE "database"."namespace_books" ALTER COLUMN "tags" TYPE JSONB', $this->statements[0] ?? '');
    }

    private function relationship(string $key, RelationshipType $type, bool $twoWay, RelationshipSide $side): Attribute
    {
        return Attribute::fromArray(['key' => $key, 'type' => ColumnType::Relationship, 'options' => [
            'relatedCollection' => 'related_' . $key,
            'relationType' => $type->value,
            'twoWay' => $twoWay,
            'twoWayKey' => 'back_' . $key,
            'side' => $side->value,
        ]]);
    }

    private function adapter(): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
