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
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
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
            $this->relationship('tags', RelationType::ManyToMany, twoWay: true, side: RelationSide::Parent),
            $this->relationship('cover', RelationType::OneToOne, twoWay: false, side: RelationSide::Child),
            $this->relationship('chapters', RelationType::OneToMany, twoWay: true, side: RelationSide::Parent),
            $this->relationship('shelf', RelationType::ManyToOne, twoWay: true, side: RelationSide::Child),
            $this->relationship('isbn', RelationType::OneToOne, twoWay: false, side: RelationSide::Parent),
            $this->relationship('summary', RelationType::OneToOne, twoWay: true, side: RelationSide::Child),
            $this->relationship('series', RelationType::OneToMany, twoWay: true, side: RelationSide::Child),
            $this->relationship('publisher', RelationType::ManyToOne, twoWay: true, side: RelationSide::Parent),
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
        yield 'index' => [IndexType::Index];
    }

    #[DataProvider('unknownIndexTypes')]
    public function testCreateIndexRefusesATypeTheEngineDoesNotCreate(IndexType $type): void
    {
        try {
            $this->adapter()->createIndex('events', new Index('happened_index', $type, ['happened']));
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
            $this->adapter()->createIndex('books', new Index('meta_index', IndexType::Object, [$path]), [$path => ColumnType::Object->value]);
            $this->fail('A nested index path with an invalid segment must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Invalid JSON key ' . $segment, $error->getMessage());
        }

        $this->assertSame([], $this->statements);
    }

    public function testANestedObjectIndexPathIsIndexedAsText(): void
    {
        $this->adapter()->createIndex('books', new Index('meta_index', IndexType::Object, ['meta.inner.leaf-key']), ['meta.inner.leaf-key' => ColumnType::Object->value]);

        $this->assertSame(
            ['CREATE INDEX "namespace__books_meta_index" ON "database"."namespace_books" USING GIN ((("meta"->\'inner\'->>\'leaf-key\')::text))'],
            $this->statements,
        );
    }

    public function testUpdatingAnArrayAttributeKeepsItsColumnJsonb(): void
    {
        $this->adapter()->updateAttribute('books', Attribute::string('tags', size: 64, array: true));

        $this->assertSame('ALTER TABLE "database"."namespace_books" ALTER COLUMN "tags" TYPE JSONB', $this->statements[0] ?? '');
    }

    private function relationship(string $key, RelationType $type, bool $twoWay, RelationSide $side): Attribute
    {
        return Attribute::relationship(key: $key, options: [
            'relatedCollection' => 'related_' . $key,
            'relationType' => $type->value,
            'twoWay' => $twoWay,
            'twoWayKey' => 'back_' . $key,
            'side' => $side->value,
        ]);
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
