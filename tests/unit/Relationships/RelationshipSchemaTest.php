<?php

namespace Tests\Unit\Relationships;

use Closure;
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
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;

final class RelationshipSchemaTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRenamingARelationshipWhoseIndexIsGoneKeepsTheOldKey(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));
        $database->deleteIndex('books', '_index_author');

        try {
            $database->updateRelationship('books', 'author', newKey: 'writer');
            $this->fail('a relationship whose index is gone cannot be renamed');
        } catch (DatabaseException $error) {
            $this->assertSame("Failed to update relationship indexes for 'author': Index not found", $error->getMessage());
            $this->assertInstanceOf(NotFoundException::class, $error->getPrevious());
        }

        $keys = $this->attributeKeys($database, 'books');
        $this->assertContains('author', $keys);
        $this->assertNotContains('writer', $keys);
        $this->assertSame('author', $this->relationship($database, 'authors', 'books')->twoWayKey);
    }

    public function testIndexMetadataOfTheMetadataCollectionCannotBeUpdated(): void
    {
        $database = new class (new Memory(), new Cache(new None())) extends Database {
            public function renameIndexAttributes(string $collection, string $id): Index
            {
                return $this->updateIndexMeta($collection, $id, static function (Index $index): void {
                    $index->setAttribute('attributes', ['changed']);
                });
            }
        };
        $this->prepare($database);

        try {
            $database->renameIndexAttributes(Database::METADATA, '_key_title');
            $this->fail('the metadata collection\'s indexes must not be changed');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot update metadata indexes', $error->getMessage());
        }

        try {
            $database->renameIndexAttributes('books', 'missing');
            $this->fail('an unknown index cannot be changed');
        } catch (NotFoundException $error) {
            $this->assertSame('Index not found', $error->getMessage());
        }
    }

    public function testAFailedDefinitionUpdateReversesTheSchemaRename(): void
    {
        $renames = [];
        $failure = new RuntimeException('the related definition could not be written');
        $adapter = $this->memory([
            'updateRelationship' => static function (Relationship $relationship, ?string $newKey, ?string $newTwoWayKey) use (&$renames): ?bool {
                $renames[] = "{$relationship->key}->{$newKey}";

                return null;
            },
        ]);
        $database = $this->intercepting($adapter, attributeMeta: static function (string $collection, string $id) use ($failure): void {
            if ($collection === 'authors' && $id === 'books') {
                throw $failure;
            }
        });
        $database->createRelationship(Relationship::manyToOne(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'author', twoWayKey: 'books'));

        try {
            $database->updateRelationship('books', 'author', newKey: 'writer');
            $this->fail('a failed definition update must fail the rename');
        } catch (RuntimeException $error) {
            $this->assertSame($failure, $error);
        }

        $this->assertSame(['author->writer', 'writer->author'], $renames);
        $this->assertContains('author', $this->attributeKeys($database, 'books'), 'the definition that was written is restored with the schema');
        $this->assertNotContains('writer', $this->attributeKeys($database, 'books'));
        $this->assertSame('books', $this->relationship($database, 'books', 'author')->twoWayKey);
        $this->assertSame('author', $this->relationship($database, 'authors', 'books')->twoWayKey);
    }

    public function testAFailedJunctionDefinitionUpdateRestoresBothSides(): void
    {
        $failure = new RuntimeException('the junction definition could not be written');
        $database = $this->intercepting(new Memory(), attributeMeta: static function (string $collection, string $id) use ($failure): void {
            if (\str_starts_with($collection, '_') && $id === 'writers') {
                throw $failure;
            }
        });
        $database->createRelationship(Relationship::manyToMany(collection: 'books', relatedCollection: 'authors', twoWay: true, key: 'writers', twoWayKey: 'works'));

        try {
            $database->updateRelationship('books', 'writers', newKey: 'authors_of', newTwoWayKey: 'written');
            $this->fail('a failed junction definition update must fail the rename');
        } catch (RuntimeException $error) {
            $this->assertSame($failure, $error);
        }

        $this->assertContains('writers', $this->attributeKeys($database, 'books'));
        $this->assertNotContains('authors_of', $this->attributeKeys($database, 'books'));
        $this->assertContains('works', $this->attributeKeys($database, 'authors'));
        $this->assertNotContains('written', $this->attributeKeys($database, 'authors'));
        $this->assertSame('works', $this->relationship($database, 'books', 'writers')->twoWayKey);
        $this->assertSame('writers', $this->relationship($database, 'authors', 'works')->twoWayKey);
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_values(\array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $this->attributes($database, $collection),
        ));
    }

    private function relationship(Database $database, string $collection, string $key): Relationship
    {
        foreach ($this->attributes($database, $collection) as $attribute) {
            if ($attribute->key === $key) {
                return Relationship::fromArray(['collection' => $collection] + $attribute->getArrayCopy());
            }
        }

        $this->fail("{$collection} has no relationship {$key}");
    }

    /**
     * @return array<Attribute>
     */
    private function attributes(Database $database, string $collection): array
    {
        /** @var array<Attribute> $attributes */
        $attributes = $database->getCollection($collection)->getAttribute('attributes', []);

        return $attributes;
    }

    private function database(Adapter $adapter): Database
    {
        return $this->prepare(new Database($adapter, new Cache(new None())));
    }

    private function prepare(Database $database): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_schema')
            ->setNamespace('relationship_schema_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
        $database->createCollection(new Collection(id: 'books', attributes: [Attribute::string(key: 'title', size: 64)], permissions: $permissions));
        $database->createCollection(new Collection(id: 'authors', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $permissions));

        return $database;
    }

    /**
     * @param  array<string, Closure>  $overrides
     */
    private function memory(array $overrides): Memory
    {
        return new class ($overrides) extends Memory {
            /**
             * @param  array<string, Closure>  $overrides
             */
            public function __construct(private readonly array $overrides)
            {
                parent::__construct();
            }

            public function createRelationship(Relationship $relationship): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship]) ?? parent::createRelationship($relationship);
            }

            public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship, $newKey, $newTwoWayKey]) ?? parent::updateRelationship($relationship, $newKey, $newTwoWayKey);
            }

            public function deleteRelationship(Relationship $relationship): bool
            {
                return $this->intercept(__FUNCTION__, [$relationship]) ?? parent::deleteRelationship($relationship);
            }

            public function deleteCollection(string $id): bool
            {
                if (! \str_starts_with($id, '_')) {
                    return parent::deleteCollection($id);
                }

                return $this->intercept(__FUNCTION__, [$id]) ?? parent::deleteCollection($id);
            }

            public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool
            {
                return $this->intercept(__FUNCTION__, [$collection, $index]) ?? parent::createIndex($collection, $index, $indexAttributeTypes, $collation);
            }

            public function deleteIndex(string $collection, string $id): bool
            {
                return $this->intercept(__FUNCTION__, [$collection, $id]) ?? parent::deleteIndex($collection, $id);
            }

            /**
             * @param  list<mixed>  $arguments
             */
            private function intercept(string $method, array $arguments): ?bool
            {
                $override = $this->overrides[$method] ?? null;
                $result = $override === null ? null : $override(...$arguments);

                return \is_bool($result) ? $result : null;
            }
        };
    }

    /**
     * @param  (Closure(string, string, Document): void)|null  $update
     * @param  (Closure(string, string): void)|null  $attributeMeta
     * @param  (Closure(string): void)|null  $create
     */
    private function intercepting(Adapter $adapter, ?Closure $update = null, ?Closure $attributeMeta = null, ?Closure $create = null): Database
    {
        $database = new class ($adapter, new Cache(new None()), $update, $attributeMeta, $create) extends Database {
            public function __construct(
                Adapter $adapter,
                Cache $cache,
                private readonly ?Closure $update,
                private readonly ?Closure $attributeMeta,
                private readonly ?Closure $create,
            ) {
                parent::__construct($adapter, $cache);
            }

            public function updateDocument(string $collection, string $id, Document $document): Document
            {
                if ($this->update !== null) {
                    ($this->update)($collection, $id, $document);
                }

                return parent::updateDocument($collection, $id, $document);
            }

            public function createDocument(string $collection, Document $document): Document
            {
                if ($this->create !== null) {
                    ($this->create)($collection);
                }

                return parent::createDocument($collection, $document);
            }

            protected function updateAttributeMeta(string $collection, string $id, callable $updateCallback, bool $triggerEvent = true): Attribute
            {
                if ($this->attributeMeta !== null) {
                    ($this->attributeMeta)($collection, $id);
                }

                return parent::updateAttributeMeta($collection, $id, $updateCallback, $triggerEvent);
            }
        };

        return $this->prepare($database);
    }
}
