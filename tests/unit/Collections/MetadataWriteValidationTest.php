<?php

namespace Tests\Unit\Collections;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\IndexType;

final class MetadataWriteValidationTest extends TestCase
{
    private const int OVERSIZED_NAME_LENGTH = 257;

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
            'memory' => [static fn (): Adapter => new Memory()],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateAttributeValidatesTheMetadataDocumentLikeCreateCollection(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->createUnvalidatedCollection($database);

        try {
            $database->createAttribute('unvalidated', Attribute::integer(key: 'age'));
            $this->fail('createAttribute() must not re-persist metadata that createCollection() rejects');
        } catch (DatabaseException $exception) {
            $this->assertInstanceOf(StructureException::class, $exception->getPrevious());
        }

        $this->assertSame([], $this->keys($database, 'unvalidated'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateRelationshipValidatesTheMetadataDocumentsLikeCreateCollection(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->createUnvalidatedCollection($database);
        $database->createCollection(new Collection(id: 'related'));

        try {
            $database->createRelationship($this->relationship());
            $this->fail('createRelationship() must not re-persist metadata that createCollection() rejects');
        } catch (DatabaseException $exception) {
            $this->assertStringStartsWith('Failed to create relationship: Invalid document structure', $exception->getMessage());
        }

        $this->assertSame([], $this->keys($database, 'unvalidated'));
        $this->assertSame([], $this->keys($database, 'related'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteRelationshipValidatesTheMetadataDocumentsLikeCreateCollection(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->createUnvalidatedCollection($database);
        $database->createCollection(new Collection(id: 'related'));
        $database->skipValidation(fn (): bool => $database->createRelationship($this->relationship()));

        try {
            $database->deleteRelationship('unvalidated', 'owner');
            $this->fail('deleteRelationship() must not re-persist metadata that createCollection() rejects');
        } catch (DatabaseException $exception) {
            $this->assertStringStartsWith("Failed to persist metadata after retries for relationship deletion 'owner'", $exception->getMessage());
            $this->assertInstanceOf(StructureException::class, $exception->getPrevious());
        }

        $this->assertSame(['owner'], $this->keys($database, 'unvalidated'));
        $this->assertSame(['owned'], $this->keys($database, 'related'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreateRelationshipRollbackValidatesItsMetadataWrites(Closure $adapter): void
    {
        $database = new MetadataWriteRecorder($adapter(), new Cache(new None()));
        $this->configure($database);
        $database->createCollection(new Collection(id: 'posts', attributes: [Attribute::string(key: 'owner', size: 64)]));
        $database->createCollection(new Collection(id: 'users'));
        $database->createIndex('posts', new Index(key: '_index_author', type: IndexType::Key, attributes: ['owner']));
        $setupWrites = \count($database->getValidations());

        try {
            $database->createRelationship(new Relationship(
                collection: 'posts',
                relatedCollection: 'users',
                type: RelationType::ManyToOne,
                key: 'author',
            ));
            $this->fail('createRelationship() must fail when the index it creates already exists');
        } catch (DatabaseException $exception) {
            $this->assertSame('Failed to create relationship indexes: Index already exists', $exception->getMessage());
        }

        $this->assertSame(
            [true, true, true, true],
            \array_slice($database->getValidations(), $setupWrites),
            "The relationship and its rollback must each write both collections' metadata with validation on",
        );
        $this->assertSame(['owner'], $this->keys($database, 'posts'));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $this->configure($database);

        return $database;
    }

    private function configure(Database $database): void
    {
        $database
            ->setDatabase('metadata_write_validation')
            ->setNamespace('metadata_write_validation_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
    }

    private function createUnvalidatedCollection(Database $database): void
    {
        $collection = new Collection(id: 'unvalidated', name: \str_repeat('n', self::OVERSIZED_NAME_LENGTH));
        $database->skipValidation(fn (): Collection => $database->createCollection($collection));
    }

    private function relationship(): Relationship
    {
        return new Relationship(
            collection: 'unvalidated',
            relatedCollection: 'related',
            type: RelationType::OneToMany,
            twoWay: true,
            key: 'owner',
            twoWayKey: 'owned',
        );
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            \array_values($database->getCollection($collection)->attributes),
        );
    }
}
