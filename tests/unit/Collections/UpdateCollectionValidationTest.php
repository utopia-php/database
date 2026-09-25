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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\IndexType;

final class UpdateCollectionValidationTest extends TestCase
{
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
    public function testUpdateCollectionValidatesTheMetadataDocumentLikeCreateCollection(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $name = \str_repeat('n', 257);

        try {
            $database->createCollection(new Collection(id: 'validated', name: $name));
            $this->fail('createCollection() must reject metadata that fails structure validation');
        } catch (DatabaseException $exception) {
            $this->assertInstanceOf(StructureException::class, $exception->getPrevious());
        }

        $database->skipValidation(fn (): Collection => $database->createCollection(new Collection(id: 'unvalidated', name: $name)));

        $this->expectException(StructureException::class);

        $database->updateCollection('unvalidated', [Permission::read(Role::any())], true);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUpdateCollectionPersistsHydratedAttributesIndexesAndRelationships(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createCollection(new Collection(
            id: 'books',
            attributes: [
                Attribute::string('title', size: 64, required: true),
                Attribute::integer('pages', required: false),
                Attribute::string('tags', size: 16, required: false, array: true),
            ],
            indexes: [new Index(key: 'title_index', type: IndexType::Key, attributes: ['title'])],
            permissions: [Permission::create(Role::any())],
        ));
        $database->createCollection(new Collection(id: 'authors', attributes: [Attribute::string('name', size: 64, required: false)]));
        $database->createRelationship(new Relationship(
            collection: 'books',
            relatedCollection: 'authors',
            type: RelationType::ManyToOne,
            twoWay: true,
            key: 'author',
            twoWayKey: 'books',
        ));

        $permissions = [Permission::read(Role::any()), Permission::update(Role::any())];
        $database->updateCollection('books', $permissions, false);

        $books = $database->getCollection('books');
        $this->assertSame($permissions, $books->getPermissions());
        $this->assertFalse($books->getAttribute('documentSecurity'));
        $this->assertSame(['title', 'pages', 'tags', 'author'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $books->attributes));
        $this->assertSame(['title_index', '_index_author'], \array_map(static fn (Index $index): string => $index->key, $books->indexes));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('update_collection')
            ->setNamespace('update_collection_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();

        return $database;
    }
}
