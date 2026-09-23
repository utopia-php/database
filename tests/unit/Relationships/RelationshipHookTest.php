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
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ForeignKeyAction;

final class RelationshipHookTest extends TestCase
{
    private const ADMIN = 'user:admin';

    /**
     * @return iterable<string, array{Closure(): Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'memory' => [static fn (): Adapter => new Memory()];
        yield 'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testOneToManyCascadeDeletesMoreChildrenThanTheQueryValueLimit(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::oneToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: ForeignKeyAction::Cascade));

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id, 'parent' => 'parent1']));
        }

        $database->setMaxQueryValues(2);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertTrue($database->getDocument('parent', 'parent1')->isEmpty());
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testManyToOneCascadeDeletesMoreChildrenThanTheQueryValueLimit(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::manyToOne(collection: 'child', relatedCollection: 'parent', twoWay: true, key: 'parent', twoWayKey: 'children', onDelete: ForeignKeyAction::Cascade));

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id, 'parent' => 'parent1']));
        }

        $database->setMaxQueryValues(2);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertTrue($database->getDocument('parent', 'parent1')->isEmpty());
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testManyToManyCascadeDeletesMoreRelatedDocumentsThanTheQueryValueLimit(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::manyToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: ForeignKeyAction::Cascade));

        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id]));
        }
        $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['child1', 'child2', 'child3']]));

        $database->setMaxQueryValues(2);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertTrue($database->getDocument('parent', 'parent1')->isEmpty());
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testManyToManySetNullDeletesMoreJunctionRowsThanTheQueryValueLimit(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::manyToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: ForeignKeyAction::SetNull));

        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id]));
        }
        $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['child1', 'child2', 'child3']]));
        $database->createDocument('parent', new Document(['$id' => 'parent2', 'children' => ['child1']]));

        $database->setMaxQueryValues(2);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertTrue($database->getDocument('parent', 'parent1')->isEmpty());
        $this->assertSame(['child1', 'child2', 'child3'], $this->ids($database, 'child'));
        $this->assertSame(['parent2'], $this->relatedIds($database->getDocument('child', 'child1'), 'parents'));
        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'child2'), 'parents'));
        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'child3'), 'parents'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testOneToManyCascadeRollsBackWhenAChildCannotBeDeleted(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::oneToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: ForeignKeyAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'deletable', 'parent' => 'parent1', '$permissions' => [Permission::delete(Role::any())]]));
        $database->createDocument('child', new Document(['$id' => 'protected', 'parent' => 'parent1', '$permissions' => [Permission::delete(Role::user('admin'))]]));

        $this->assertDeleteRejected($database, 'parent', 'parent1');

        $this->assertFalse($database->getDocument('parent', 'parent1')->isEmpty());
        $this->assertSame(['deletable', 'protected'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));

        $database->getAuthorization()->addRole(self::ADMIN);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testManyToOneCascadeRollsBackWhenAChildCannotBeDeleted(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::manyToOne(collection: 'child', relatedCollection: 'parent', twoWay: true, key: 'parent', twoWayKey: 'children', onDelete: ForeignKeyAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'deletable', 'parent' => 'parent1', '$permissions' => [Permission::delete(Role::any())]]));
        $database->createDocument('child', new Document(['$id' => 'protected', 'parent' => 'parent1', '$permissions' => [Permission::delete(Role::user('admin'))]]));

        $this->assertDeleteRejected($database, 'parent', 'parent1');

        $this->assertFalse($database->getDocument('parent', 'parent1')->isEmpty());
        $this->assertSame(['deletable', 'protected'], $this->ids($database, 'child'));

        $database->getAuthorization()->addRole(self::ADMIN);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testManyToManyCascadeRollsBackWhenARelatedDocumentCannotBeDeleted(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::manyToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: ForeignKeyAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('child', new Document(['$id' => 'deletable', '$permissions' => [Permission::delete(Role::any())]]));
        $database->createDocument('child', new Document(['$id' => 'protected', '$permissions' => [Permission::delete(Role::user('admin'))]]));
        $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['deletable', 'protected']]));

        $this->assertDeleteRejected($database, 'parent', 'parent1');

        $this->assertSame(['deletable', 'protected'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
        $this->assertSame(['deletable', 'protected'], $this->ids($database, 'child'));

        $database->getAuthorization()->addRole(self::ADMIN);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCascadeSkipsARelatedDocumentThatIsAlreadyGone(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::manyToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: ForeignKeyAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id, '$permissions' => [Permission::delete(Role::any())]]));
        }
        $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['child1', 'child2', 'child3']]));

        $database->skipRelationships(fn () => $database->deleteDocument('child', 'child2'));

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCascadeRetriedAfterAFailedCascadeStillDeletesTheChildren(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::oneToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: ForeignKeyAction::Cascade));
        $database->createCollection(new Collection(id: 'grandchild', permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship(Relationship::oneToMany(collection: 'child', relatedCollection: 'grandchild', twoWay: true, key: 'grandchildren', twoWayKey: 'child', onDelete: ForeignKeyAction::Restrict));

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'child1', 'parent' => 'parent1']));
        $database->createDocument('grandchild', new Document(['$id' => 'grandchild1', 'child' => 'child1']));

        try {
            $database->deleteDocument('parent', 'parent1');
            $this->fail('A restricted grandchild must stop the cascade');
        } catch (RestrictedException) {
        }

        $database->deleteDocument('grandchild', 'grandchild1');

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function database(Closure $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter(), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_hook')
            ->setNamespace('relationship_hook_'.\uniqid());

        $database->create();
        $database->addHook(new Relationships($database));
        $database->addHook(new Permissions());

        return $database;
    }

    /**
     * @param  array<string>  $childPermissions
     */
    private function relate(Database $database, Relationship $relationship, array $childPermissions = [], bool $childDocumentSecurity = true): void
    {
        $database->createCollection(new Collection(id: 'parent', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(new Collection(id: 'child', permissions: $childPermissions === [] ? $this->permissions() : $childPermissions, documentSecurity: $childDocumentSecurity));
        $database->createRelationship($relationship);
    }

    /**
     * @return array<string>
     */
    private function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    private function assertDeleteRejected(Database $database, string $collection, string $id): void
    {
        try {
            $database->deleteDocument($collection, $id);
            $this->fail('Cascading into a document the caller may not delete must be rejected');
        } catch (AuthorizationException $exception) {
            $this->assertSame('Missing "delete" permission for role "user:admin". Only "["any"]" scopes are allowed and "["user:admin"]" was given.', $exception->getMessage());
        }
    }

    /**
     * @return array<string>
     */
    private function ids(Database $database, string $collection): array
    {
        $ids = \array_map(
            fn (Document $document) => $document->getId(),
            $database->getAuthorization()->skip(fn () => $database->skipRelationships(fn () => $database->find($collection))),
        );
        \sort($ids);

        return $ids;
    }

    /**
     * @return array<string>
     */
    private function relatedIds(Document $document, string $key): array
    {
        $ids = \array_map(fn (Document $related) => $related->getId(), $document->getDocuments($key));
        \sort($ids);

        return $ids;
    }
}
