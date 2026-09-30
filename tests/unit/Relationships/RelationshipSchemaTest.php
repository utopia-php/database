<?php

namespace Tests\Unit\Relationships;

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
}
