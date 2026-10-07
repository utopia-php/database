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
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Operator;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;

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
        $this->relate($database, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::Cascade));

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
        $this->relate($database, Relationship::manyToOne(relatedCollection: 'parent', twoWay: true, key: 'parent', twoWayKey: 'children', onDelete: RelationshipDeleteAction::Cascade));

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
        $this->relate($database, Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::Cascade));

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
        $this->relate($database, Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull));

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
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::Cascade),
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
            Relationship::manyToOne(relatedCollection: 'parent', twoWay: true, key: 'parent', twoWayKey: 'children', onDelete: RelationshipDeleteAction::Cascade),
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
            Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('child', new Document(['$id' => 'deletable', '$permissions' => [Permission::delete(Role::any())]]));
        $database->createDocument('child', new Document(['$id' => 'protected', '$permissions' => [Permission::delete(Role::user('admin'))]]));
        $database->getAuthorization()->skip(fn () => $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['deletable', 'protected']])));

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
            Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id, '$permissions' => [Permission::delete(Role::any())]]));
        }
        $database->getAuthorization()->skip(fn () => $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['child1', 'child2', 'child3']])));

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
        $this->relate($database, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::Cascade));
        $database->createCollection(Collection::create(id: 'grandchild', permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('child', Relationship::oneToMany(relatedCollection: 'grandchild', twoWay: true, key: 'grandchildren', twoWayKey: 'child', onDelete: RelationshipDeleteAction::Restrict));

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
    #[DataProvider('adapters')]
    public function testLinkingAChildWithoutUpdatePermissionIsRejected(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'updatable', '$permissions' => [Permission::update(Role::any())]]));
        $database->createDocument('child', new Document(['$id' => 'readonly', '$permissions' => [Permission::update(Role::user('admin'))]]));

        try {
            $database->updateDocument('parent', 'parent1', new Document(['children' => ['updatable', 'readonly']]));
            $this->fail('Linking a child the caller may not update must be rejected');
        } catch (AuthorizationException $exception) {
            $this->assertSame('Missing "update" permission for role "user:admin". Only "["any"]" scopes are allowed and "["user:admin"]" was given.', $exception->getMessage());
        }

        $this->assertSame([], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));

        $database->getAuthorization()->addRole(self::ADMIN);

        $database->updateDocument('parent', 'parent1', new Document(['children' => ['updatable', 'readonly']]));
        $this->assertSame(['readonly', 'updatable'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testNestedUpdateCannotLinkAChildWithoutUpdatePermission(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );
        $database->createCollection(Collection::create(id: 'grandparent', permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('grandparent', Relationship::oneToOne(relatedCollection: 'parent', key: 'parent', onDelete: RelationshipDeleteAction::SetNull));

        $database->createDocument('child', new Document(['$id' => 'readonly', '$permissions' => [Permission::update(Role::user('admin'))]]));
        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('grandparent', new Document(['$id' => 'grandparent1', 'parent' => 'parent1']));

        try {
            $database->updateDocument('grandparent', 'grandparent1', new Document(['parent' => new Document(['$id' => 'parent1', 'children' => ['readonly']])]));
            $this->fail('Linking a child the caller may not update must be rejected');
        } catch (AuthorizationException $exception) {
            $this->assertSame('Missing "update" permission for role "user:admin". Only "["any"]" scopes are allowed and "["user:admin"]" was given.', $exception->getMessage());
        }

        $this->assertSame([], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAChildGivenAsADocumentThroughANestedUpdateNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));

        $this->assertLinkRejected(fn () => $database->updateDocument('grandparent', 'grandparent1', new Document([
            'parent' => new Document(['$id' => 'parent1', 'children' => [new Document(['$id' => 'readonly'])]]),
        ])));

        $this->assertSame([], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingExistingChildrenThroughANestedCreateNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));

        $this->assertLinkRejected(fn () => $database->createDocument('grandparent', new Document([
            '$id' => 'grandparent2',
            'parent' => new Document(['$id' => 'parent2', 'children' => ['readonly']]),
        ])));

        $this->assertNull($database->getDocument('child', 'readonly')->getAttribute('parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAPartnerThroughANestedTwoWayOneToOneUpdateNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, Relationship::oneToOne(relatedCollection: 'child', twoWay: true, key: 'partner', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));

        $this->assertLinkRejected(fn () => $database->updateDocument('grandparent', 'grandparent1', new Document([
            'parent' => new Document(['$id' => 'parent1', 'partner' => 'readonly']),
        ])));

        $this->assertNull($database->getDocument('child', 'readonly')->getAttribute('parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRelinkingAnUnchangedChildNeedsOnlyReadPermission(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull),
            [Permission::create(Role::any()), Permission::read(Role::any())],
            false,
        );

        $database->createDocument('parent', new Document(['$id' => 'parent1', 'name' => 'before']));
        $database->createDocument('child', new Document(['$id' => 'child1', 'parent' => 'parent1']));

        $parent = $database->updateDocument('parent', 'parent1', new Document(['name' => 'after', 'children' => ['child1']]));

        $this->assertSame('after', $parent->getAttribute('name'));
        $this->assertSame(['child1'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRelinkingKeepsAnUnchangedChildTheCallerMayNotUpdate(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'linked', 'parent' => 'parent1', '$permissions' => [Permission::update(Role::user('admin'))]]));
        $database->createDocument('child', new Document(['$id' => 'added', '$permissions' => [Permission::update(Role::any())]]));

        $database->updateDocument('parent', 'parent1', new Document(['children' => ['linked', 'added']]));

        $this->assertSame(['added', 'linked'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRelationshipMaintenanceKeepsTheTenantOfEveryRelatedDocument(Closure $adapter): void
    {
        $database = $this->database($adapter, sharedTables: true);
        $this->relate($database, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));
        $database->setTenant(1);

        $database->createDocument('parent', new Document(['$id' => 'parent1', '$tenant' => 1]));
        $database->createDocument('child', new Document(['$id' => 'child1', '$tenant' => 1, 'parent' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'child2', '$tenant' => 1]));
        $database->createDocument('child', new Document(['$id' => 'foreign', '$tenant' => 2, 'parent' => 'parent1']));

        $database->updateDocument('parent', 'parent1', new Document(['children' => ['child2']]));
        $this->assertSame(['child2'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));

        foreach (['child1', 'child2'] as $id) {
            $child = $database->getDocument('child', $id);
            $this->assertSame(1, $child->getTenant(), "{$id} must keep its tenant");
            $this->assertNull($child->getAttribute('parent'), "{$id} must no longer reference the deleted parent");
        }

        $foreign = $database->withTenant(2, fn () => $database->skipRelationships(fn () => $database->getDocument('child', 'foreign')));
        $this->assertSame(2, $foreign->getTenant());
        $this->assertSame('parent1', $foreign->getAttribute('parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCascadeDeletesAGrandchildTheCallerCannotRead(Closure $adapter): void
    {
        $database = $this->nestedCascadeDatabase($adapter, [Permission::create(Role::any()), Permission::delete(Role::any())], RelationshipDeleteAction::Cascade);

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));

        $this->assertSame([], $this->ids($database, 'child'));
        $this->assertSame([], $this->ids($database, 'grandchild'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCascadeIsRestrictedByAGrandchildTheCallerCannotRead(Closure $adapter): void
    {
        $database = $this->nestedCascadeDatabase($adapter, [Permission::create(Role::any()), Permission::delete(Role::any())], RelationshipDeleteAction::Restrict);

        try {
            $database->deleteDocument('parent', 'parent1');
            $this->fail('Cascading into a document whose relationship restricts its delete must be rejected');
        } catch (RestrictedException) {
        }

        $this->assertSame(['parent1'], $this->ids($database, 'parent'));
        $this->assertSame(['child1'], $this->ids($database, 'child'));
        $this->assertSame(['grandchild1'], $this->ids($database, 'grandchild'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCascadeRollsBackWhenAGrandchildTheCallerCannotReadIsProtected(Closure $adapter): void
    {
        $database = $this->nestedCascadeDatabase($adapter, [Permission::create(Role::any()), Permission::delete(Role::user('admin'))], RelationshipDeleteAction::Cascade);

        $this->assertDeleteRejected($database, 'parent', 'parent1');

        $this->assertSame(['parent1'], $this->ids($database, 'parent'));
        $this->assertSame(['child1'], $this->ids($database, 'child'));
        $this->assertSame(['grandchild1'], $this->ids($database, 'grandchild'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUpdateRelationshipFiresStringOptions(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::Cascade));

        $updates = new class () implements Lifecycle {
            /** @var array<string, mixed> */
            public array $options = [];

            public function handle(Domain $event): void
            {
                if ($event instanceof Event\Attribute\Updated) {
                    $this->options[$event->collection.'.'.$event->attribute->key] = $event->attribute->toDocument()->getAttribute('options');
                }
            }
        };
        $database->addHook($updates);

        $updated = $database->updateRelationship('parent', 'children', new RelationshipUpdate(key: 'kids', onDelete: RelationshipDeleteAction::SetNull));
        $this->assertSame('kids', $updated->key);
        $this->assertSame(RelationshipDeleteAction::SetNull, $updated->onDelete);

        $this->assertSame(
            [
                'parent.kids' => [
                    'relatedCollection' => 'child',
                    'relationType' => 'oneToMany',
                    'twoWay' => true,
                    'twoWayKey' => 'parent',
                    'onDelete' => 'setNull',
                    'side' => 'parent',
                ],
                'child.parent' => [
                    'relatedCollection' => 'parent',
                    'relationType' => 'oneToMany',
                    'twoWay' => true,
                    'twoWayKey' => 'kids',
                    'onDelete' => 'setNull',
                    'side' => 'child',
                ],
            ],
            $updates->options,
        );
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  list<string>  $grandchildPermissions
     */
    private function nestedCascadeDatabase(Closure $adapter, array $grandchildPermissions, RelationshipDeleteAction $onDelete): Database
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::Cascade),
        );
        $database->createCollection(Collection::create(id: 'grandchild', permissions: $grandchildPermissions, documentSecurity: false));
        $database->createRelationship('child', Relationship::oneToMany(relatedCollection: 'grandchild', twoWay: true, key: 'grandchildren', twoWayKey: 'child', onDelete: $onDelete));

        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createDocument('parent', new Document(['$id' => 'parent1']));
            $database->createDocument('child', new Document(['$id' => 'child1', 'parent' => 'parent1']));
            $database->createDocument('grandchild', new Document(['$id' => 'grandchild1', 'child' => 'child1']));
        });

        return $database;
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function database(Closure $adapter, bool $sharedTables = false): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter(), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_hook')
            ->setNamespace('relationship_hook_'.\uniqid());

        if ($sharedTables) {
            $database
                ->setSharedTables(true)
                ->setTenantPerDocument(true)
                ->setTenant(null);
        }

        $database->create();
        $database->addHook(new Relationships());
        $database->addHook(new Permissions());

        return $database;
    }

    /**
     * @param  list<string>  $childPermissions
     */
    private function relate(Database $database, Relationship $relationship, array $childPermissions = [], bool $childDocumentSecurity = true): void
    {
        $database->createCollection(Collection::create(id: 'parent', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'child', permissions: $childPermissions === [] ? $this->permissions() : $childPermissions, documentSecurity: $childDocumentSecurity));
        $database->createRelationship($relationship->relatedCollection === 'parent' ? 'child' : 'parent', $relationship);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function nestedLinkDatabase(Closure $adapter, Relationship $relationship): Database
    {
        $database = $this->database($adapter);
        $this->relate($database, $relationship, [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createCollection(Collection::create(id: 'grandparent', permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('grandparent', Relationship::oneToOne(relatedCollection: 'parent', key: 'parent', onDelete: RelationshipDeleteAction::SetNull));

        $database->createDocument('child', new Document(['$id' => 'readonly', '$permissions' => [Permission::update(Role::user('admin'))]]));
        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('grandparent', new Document(['$id' => 'grandparent1', 'parent' => 'parent1']));

        return $database;
    }

    private function assertLinkRejected(callable $write): void
    {
        try {
            $write();
            $this->fail('Linking a document the caller may not update was accepted');
        } catch (AuthorizationException $exception) {
            $this->assertStringContainsString('"update"', $exception->getMessage());
        }
    }

    /**
     * @return list<string>
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

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAFailedNestedOneToOneWriteLeavesNoWriteStackEntry(Closure $adapter): void
    {
        $database = $this->writeStackDatabase($adapter);

        foreach ([1, 2] as $attempt) {
            $this->failNestedOneToOneWrite($database, $attempt);

            $this->assertSame(0, $database->getRelationshipHook()?->getWriteStackCount(), "Attempt {$attempt} left an entry on the write stack");
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testANestedCreateAfterAFailedNestedWriteStoresItsRelatedDocuments(Closure $adapter): void
    {
        $database = $this->writeStackDatabase($adapter);

        $this->failNestedOneToOneWrite($database, 1);
        $this->failNestedOneToOneWrite($database, 2);

        $database->createDocument('owner', new Document([
            '$id' => 'owner2',
            'items' => [new Document(['$id' => 'item1', 'details' => [new Document(['$id' => 'detail1'])]])],
        ]));

        $this->assertSame(['item1'], $this->ids($database, 'item'));
        $this->assertSame(['detail1'], $this->ids($database, 'detail'));
        $this->assertSame(['item1'], $this->relatedIds($database->getDocument('owner', 'owner2'), 'items'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function writeStackDatabase(Closure $adapter): Database
    {
        $database = $this->database($adapter);
        $database->createCollection(Collection::create(id: 'owner', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'solo', attributes: [Attribute::string(key: 'name', size: 64)], permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createCollection(Collection::create(id: 'item', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'detail', permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('owner', Relationship::oneToOne(relatedCollection: 'solo', twoWay: true, key: 'solo', twoWayKey: 'owner', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('owner', Relationship::oneToMany(relatedCollection: 'item', twoWay: true, key: 'items', twoWayKey: 'owner', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('item', Relationship::oneToMany(relatedCollection: 'detail', twoWay: true, key: 'details', twoWayKey: 'item', onDelete: RelationshipDeleteAction::SetNull));

        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createDocument('owner', new Document(['$id' => 'owner1']));
            $database->createDocument('solo', new Document(['$id' => 'solo1', 'name' => 'before']));
        });

        return $database;
    }

    private function failNestedOneToOneWrite(Database $database, int $attempt): void
    {
        try {
            $database->updateDocument('owner', 'owner1', new Document(['solo' => new Document(['$id' => 'solo1', 'name' => "attempt {$attempt}"])]));
            $this->fail('Updating a related document the caller may not update must be rejected');
        } catch (AuthorizationException $exception) {
            $this->assertSame("No permissions provided for action 'update'", $exception->getMessage());
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteDocumentsWithASelectCascadesToChildren(Closure $adapter): void
    {
        foreach ($this->deletePairs() as $type => [$relationship, $link]) {
            $database = $this->database($adapter);
            $this->relate($database, $relationship(RelationshipDeleteAction::Cascade));
            $link($database, 'parent1', 'child1');
            $link($database, 'parent2', 'child2');

            $deleted = $database->deleteDocuments('parent', [Query::equal('$id', ['parent2']), Query::select(['$id', 'name'])]);

            $this->assertSame(1, $deleted, $type);
            $this->assertSame(['parent1'], $this->ids($database, 'parent'), $type);
            $this->assertSame(['child1'], $this->ids($database, 'child'), "{$type}: the deleted parent's child must be deleted with it");
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteDocumentsWithASelectHonoursRestrict(Closure $adapter): void
    {
        foreach ($this->deletePairs() as $type => [$relationship, $link]) {
            $database = $this->database($adapter);
            $this->relate($database, $relationship(RelationshipDeleteAction::Restrict));
            $link($database, 'parent1', 'child1');
            $link($database, 'parent2', 'child2');

            try {
                $database->deleteDocuments('parent', [Query::equal('$id', ['parent2']), Query::select(['$id', 'name'])]);
                $this->fail("{$type}: deleting a parent with a related document must be restricted");
            } catch (RestrictedException) {
            }

            $this->assertSame(['parent1', 'parent2'], $this->ids($database, 'parent'), $type);
            $this->assertSame(['child1', 'child2'], $this->ids($database, 'child'), $type);
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteDocumentsWithASelectCascadesFromTheChildSideOfATwoWayOneToOne(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::oneToOne(relatedCollection: 'child', twoWay: true, key: 'child', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::Cascade));
        foreach (['1', '2'] as $suffix) {
            $database->createDocument('child', new Document(['$id' => "child{$suffix}"]));
            $database->createDocument('parent', new Document(['$id' => "parent{$suffix}", 'child' => "child{$suffix}"]));
        }

        $this->assertSame(1, $database->deleteDocuments('child', [Query::equal('$id', ['child2']), Query::select(['$id'])]));

        $this->assertSame(['child1'], $this->ids($database, 'child'));
        $this->assertSame(['parent1'], $this->ids($database, 'parent'));
    }

    /**
     * @return array<string, array{Closure(RelationshipDeleteAction): Relationship, Closure(Database, string, string): void}>
     */
    private function deletePairs(): array
    {
        $parentHoldsChild = function (Database $database, string $parent, string $child): void {
            $database->createDocument('child', new Document(['$id' => $child]));
            $database->createDocument('parent', new Document(['$id' => $parent, 'child' => $child]));
        };
        $parentListsChild = function (Database $database, string $parent, string $child): void {
            $database->createDocument('child', new Document(['$id' => $child]));
            $database->createDocument('parent', new Document(['$id' => $parent, 'children' => [$child]]));
        };
        $childHoldsKey = function (Database $database, string $parent, string $child): void {
            $database->createDocument('parent', new Document(['$id' => $parent]));
            $database->createDocument('child', new Document(['$id' => $child, 'parent' => $parent]));
        };

        return [
            'one-to-one' => [
                fn (RelationshipDeleteAction $onDelete): Relationship => Relationship::oneToOne(relatedCollection: 'child', key: 'child', twoWayKey: 'parent', onDelete: $onDelete),
                $parentHoldsChild,
            ],
            'one-to-many' => [
                fn (RelationshipDeleteAction $onDelete): Relationship => Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: $onDelete),
                $childHoldsKey,
            ],
            'many-to-one' => [
                fn (RelationshipDeleteAction $onDelete): Relationship => Relationship::manyToOne(relatedCollection: 'parent', twoWay: true, key: 'parent', twoWayKey: 'children', onDelete: $onDelete),
                $childHoldsKey,
            ],
            'many-to-many' => [
                fn (RelationshipDeleteAction $onDelete): Relationship => Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: $onDelete),
                $parentListsChild,
            ],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAManyToManyDocumentByIdNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, $this->manyToManyLink());

        $this->assertLinkRejected(fn () => $database->updateDocument('parent', 'parent1', new Document(['children' => ['readonly']])));

        $this->assertSame([], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'readonly'), 'parents'));

        $database->getAuthorization()->addRole(self::ADMIN);

        $database->updateDocument('parent', 'parent1', new Document(['children' => ['readonly']]));
        $this->assertSame(['readonly'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAManyToManyDocumentGivenAsADocumentNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, $this->manyToManyLink());

        $this->assertLinkRejected(fn () => $database->updateDocument('parent', 'parent1', new Document(['children' => [new Document(['$id' => 'readonly'])]])));

        $this->assertSame([], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'readonly'), 'parents'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAManyToManyDocumentThroughANestedUpdateNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, $this->manyToManyLink());

        $this->assertLinkRejected(fn () => $database->updateDocument('grandparent', 'grandparent1', new Document([
            'parent' => new Document(['$id' => 'parent1', 'children' => ['readonly']]),
        ])));

        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'readonly'), 'parents'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAnExistingManyToManyDocumentThroughACreateNeedsUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, $this->manyToManyLink());

        $this->assertLinkRejected(fn () => $database->createDocument('parent', new Document(['$id' => 'parent2', 'children' => ['readonly']])));
        $this->assertLinkRejected(fn () => $database->createDocument('parent', new Document(['$id' => 'parent3', 'children' => [new Document(['$id' => 'readonly'])]])));
        $this->assertLinkRejected(fn () => $database->createDocument('grandparent', new Document([
            '$id' => 'grandparent2',
            'parent' => new Document(['$id' => 'parent4', 'children' => ['readonly']]),
        ])));

        $this->assertSame(['parent1'], $this->ids($database, 'parent'));
        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'readonly'), 'parents'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreatingAManyToManyDocumentThroughALinkNeedsNoUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, $this->manyToManyLink());

        $database->updateDocument('parent', 'parent1', new Document(['children' => [new Document(['$id' => 'created'])]]));
        $database->createDocument('parent', new Document(['$id' => 'parent2', 'children' => [new Document(['$id' => 'nested'])]]));

        $this->assertSame(['created'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
        $this->assertSame(['nested'], $this->relatedIds($database->getDocument('parent', 'parent2'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testKeepingOrUnlinkingAManyToManyDocumentNeedsNoUpdatePermission(Closure $adapter): void
    {
        $database = $this->nestedLinkDatabase($adapter, $this->manyToManyLink());
        $database->getAuthorization()->skip(fn () => $database->updateDocument('parent', 'parent1', new Document(['children' => ['readonly']])));

        $database->updateDocument('parent', 'parent1', new Document(['name' => 'kept', 'children' => ['readonly']]));
        $this->assertSame(['readonly'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));

        $database->updateDocument('parent', 'parent1', new Document(['children' => []]));
        $this->assertSame([], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
        $this->assertSame([], $this->relatedIds($database->getDocument('child', 'readonly'), 'parents'));
    }

    private function manyToManyLink(): Relationship
    {
        return Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testANewNestedManyToManyDocumentKeepsItsOwnPermissions(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull));

        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => [Permission::read(Role::any())]]));

        $own = [Permission::read(Role::any()), Permission::update(Role::user('owner'))];
        $database->updateDocument('parent', 'parent1', new Document([
            'children' => [new Document(['$id' => 'child1', '$permissions' => $own])],
        ]));

        $child = $database->getAuthorization()->skip(fn () => $database->getDocument('child', 'child1'));
        $this->assertSame($own, $child->getPermissions(), 'A nested many-to-many document created with its own permissions must keep them');
        $this->assertSame(['child1'], $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testANewNestedManyToManyDocumentWithoutPermissionsTakesTheParentPermissions(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull));

        $parentPermissions = [Permission::read(Role::any()), Permission::update(Role::user('owner'))];
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $parentPermissions]));

        $database->updateDocument('parent', 'parent1', new Document([
            'children' => [new Document(['$id' => 'child1'])],
        ]));

        $child = $database->getAuthorization()->skip(fn () => $database->getDocument('child', 'child1'));
        $this->assertSame($parentPermissions, $child->getPermissions());
    }

    public function testNestedPathFiltersStayWithinTheQueryValueLimit(): void
    {
        $adapter = new class () extends Memory {
            /** @var array<string, int> */
            public array $largestValueCounts = [];

            #[\Override]
            public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
            {
                $this->largestValueCounts[$collection->getId()] = \max($this->largestValueCounts[$collection->getId()] ?? 0, $this->largestValueCount($queries));

                return parent::find($collection, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $forPermission);
            }

            public function forgetValueCounts(): void
            {
                $this->largestValueCounts = [];
            }

            /**
             * @param  array<mixed>  $queries
             */
            private function largestValueCount(array $queries): int
            {
                $largest = 0;
                foreach ($queries as $query) {
                    if (! $query instanceof Query) {
                        continue;
                    }
                    $largest = \max($largest, $query->isNested() ? $this->largestValueCount($query->getValues()) : \count($query->getValues()));
                }

                return $largest;
            }
        };

        $database = $this->database(fn (): Adapter => $adapter);
        $database->createCollection(Collection::create(id: 'parent', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'child', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'tag', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'label', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'owner', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('parent', Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('child', Relationship::manyToMany(relatedCollection: 'tag', twoWay: true, key: 'tags', twoWayKey: 'children', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('tag', Relationship::oneToMany(relatedCollection: 'label', twoWay: true, key: 'labels', twoWayKey: 'tag', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('child', Relationship::manyToOne(relatedCollection: 'owner', twoWay: true, key: 'owner', twoWayKey: 'children', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('parent', Relationship::manyToMany(relatedCollection: 'tag', twoWay: true, key: 'topics', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull));

        foreach (\range(1, 6) as $number) {
            $name = $number === 6 ? 'other' : 'match';
            $database->createDocument('owner', new Document(['$id' => "owner{$number}", 'name' => $name]));
            $database->createDocument('tag', new Document(['$id' => "tag{$number}"]));
            $database->createDocument('label', new Document(['$id' => "label{$number}", 'name' => $name, 'tag' => "tag{$number}"]));
            $database->createDocument('child', new Document(['$id' => "child{$number}", 'tags' => ["tag{$number}"], 'owner' => "owner{$number}"]));
            $database->createDocument('parent', new Document(['$id' => "parent{$number}", 'children' => ["child{$number}"], 'topics' => ["tag{$number}"]]));
        }

        $database->setMaxQueryValues(2);

        $matching = ['parent1', 'parent2', 'parent3', 'parent4', 'parent5'];
        $filters = [
            'children.tags.labels.name' => [['match'], $matching, ['child', 'junction', 'label']],
            'children.owner.name' => [['match'], $matching, ['child', 'owner']],
            'topics.labels.name' => [['match'], $matching, ['junction', 'label', 'tag']],
            'children.$id' => [['child1', 'child2'], ['parent1', 'parent2'], ['child']],
        ];
        foreach ($filters as $path => [$values, $expected, $collections]) {
            $adapter->forgetValueCounts();

            $ids = \array_map(fn (Document $parent): string => $parent->getId(), $database->find('parent', [Query::equal($path, $values), Query::select(['$id'])]));
            \sort($ids);
            $this->assertSame($expected, $ids, "Filtering by {$path}");

            unset($adapter->largestValueCounts['parent']);
            $collectionsRead = \array_values(\array_unique(\array_map(fn (string $collection): string => \str_starts_with($collection, '_') ? 'junction' : $collection, \array_keys($adapter->largestValueCounts))));
            \sort($collectionsRead);
            $this->assertSame($collections, $collectionsRead, "Filtering by {$path} reads only the collections on the path");
            foreach ($adapter->largestValueCounts as $collection => $largest) {
                $this->assertLessThanOrEqual(2, $largest, "A read of {$collection} while filtering by {$path} carried {$largest} values");
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCascadeWithOnlyADanglingJunctionRowDeletesTheParent(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate(
            $database,
            Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::Cascade),
            [Permission::create(Role::any()), Permission::read(Role::any())],
            false,
        );
        $junction = '_'.$database->getCollection('parent')->getSequence().'_'.$database->getCollection('child')->getSequence();

        $database->getAuthorization()->skip(function () use ($database): void {
            $database->createDocument('child', new Document(['$id' => 'child1']));
            $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['child1']]));
            $database->skipRelationships(fn () => $database->deleteDocument('child', 'child1'));
        });

        $this->assertSame([], $this->ids($database, 'child'));
        $this->assertCount(1, $this->ids($database, $junction), 'The junction row must outlive the child it points at');

        $this->assertTrue($database->deleteDocument('parent', 'parent1'));
        $this->assertSame([], $this->ids($database, 'parent'));
        $this->assertSame([], $this->ids($database, $junction));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testCreatingWithAListOnTheChildSideOfAOneWayOneToOneIsRejected(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $this->relate($database, Relationship::oneToOne(relatedCollection: 'child', key: 'partner', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));
        $database->createDocument('parent', new Document(['$id' => 'parent1']));

        try {
            $database->createDocument('child', new Document(['$id' => 'child1', 'parent' => ['parent1']]));
            $this->fail('A list on the child side of a one-way one-to-one must be rejected');
        } catch (RelationshipException $exception) {
            $this->assertSame('Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.', $exception->getMessage());
        }

        $this->assertSame([], $this->ids($database, 'child'));
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, Relationship, string, bool, array<string, mixed>, string, bool}>
     */
    public static function invalidRelationshipUpdates(): iterable
    {
        $oneToOne = Relationship::oneToOne(relatedCollection: 'child', key: 'partner', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull);
        $twoWayOneToOne = Relationship::oneToOne(relatedCollection: 'child', twoWay: true, key: 'partner', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull);
        $oneToMany = Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull);
        $manyToOne = Relationship::manyToOne(relatedCollection: 'child', twoWay: true, key: 'child', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull);
        $manyToMany = Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull);

        $cases = [
            'one-way one-to-one child side' => [$oneToOne, 'child', false, ['parent' => 'parent1'], 'Invalid relationship value. Cannot set a value from the child side of a oneToOne relationship when twoWay is false.', false],
            'two-way one-to-one integer' => [$twoWayOneToOne, 'parent', false, ['partner' => 123], 'Invalid relationship value. Must be either a document, document ID or null.', false],
            'two-way one-to-one list' => [$twoWayOneToOne, 'parent', false, ['partner' => ['child1']], 'Invalid relationship value. Must be either a document, document ID or null.', false],
            'one-to-many list item' => [$oneToMany, 'parent', false, ['children' => [123]], 'Invalid relationship value. Must be either a document or document ID.', false],
            'many-to-many list item' => [$manyToMany, 'parent', false, ['children' => [123]], 'Invalid relationship value. Must be either a document or document ID.', false],
            'many-to-one document without id' => [$manyToOne, 'parent', false, ['child' => new Document(['name' => 'n'])], 'Invalid relationship value. Document must have a valid $id.', false],
            'many-to-one empty scalar' => [$manyToOne, 'parent', true, ['child' => false], 'Invalid relationship value. Must be either a document ID or a document.', false],
            'many-to-one scalar' => [$manyToOne, 'parent', false, ['child' => 123], 'Invalid relationship value.', false],
            'many-to-many bulk string' => [$manyToMany, 'parent', false, ['children' => 'child1'], 'Invalid relationship value. Must be an array of documents or document IDs.', true],
        ];

        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach ($cases as $caseName => [$relationship, $collection, $linked, $update, $message, $bulk]) {
                yield "{$adapterName}: {$caseName}" => [$adapter, $relationship, $collection, $linked, $update, $message, $bulk];
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  array<string, mixed>  $update
     */
    #[DataProvider('invalidRelationshipUpdates')]
    public function testUpdateRejectsInvalidRelationshipValues(Closure $adapter, Relationship $relationship, string $collection, bool $linked, array $update, string $message, bool $bulk): void
    {
        $database = $this->database($adapter);
        $this->relate($database, $relationship);
        $database->createDocument('child', new Document(['$id' => 'child1']));
        $key = $relationship->key ?? $this->fail('an update case relates under a key');
        $database->createDocument('parent', new Document(['$id' => 'parent1', ...($linked ? [$key => 'child1'] : [])]));

        $id = $collection === 'parent' ? 'parent1' : 'child1';
        $stored = fn (): array => $database->getAuthorization()->skip(fn () => $database->skipRelationships(fn () => $database->getDocument($collection, $id)))->getArrayCopy();
        $before = $stored();

        try {
            if ($bulk) {
                $database->updateDocuments($collection, new Document($update));
            } else {
                $database->updateDocument($collection, $id, new Document($update));
            }
            $this->fail('An invalid relationship value must be rejected');
        } catch (RelationshipException $exception) {
            $this->assertSame($message, $exception->getMessage());
        }

        $this->assertSame($before, $stored());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testSelectingNestedAttributesThroughTheChildSideOfAManyToOne(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $database->createCollection(Collection::create(id: 'store', attributes: [Attribute::string(key: 'name', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'product', attributes: [Attribute::string(key: 'name', size: 64), Attribute::string(key: 'sku', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('product', Relationship::manyToOne(relatedCollection: 'store', twoWay: true, key: 'store', twoWayKey: 'products', onDelete: RelationshipDeleteAction::SetNull));

        $database->createDocument('store', new Document(['$id' => 'store1', 'name' => 'Store 1']));
        foreach (['product1', 'product2'] as $id) {
            $database->createDocument('product', new Document(['$id' => $id, 'name' => "Name {$id}", 'sku' => "sku-{$id}", 'store' => 'store1']));
        }

        $stores = [
            'getDocument' => $database->getDocument('store', 'store1', [Query::select(['*', 'products.name'])]),
            'findOne' => $database->findOne('store', [Query::select(['*', 'products.name'])]),
        ];
        foreach ($stores as $read => $store) {
            $this->assertSame('Store 1', $store->getAttribute('name'), $read);
            $products = $store->getDocuments('products');
            $this->assertSame(['product1', 'product2'], $this->relatedIds($store, 'products'), $read);
            foreach ($products as $product) {
                $this->assertSame("Name {$product->getId()}", $product->getAttribute('name'), $read);
                $this->assertFalse($product->offsetExists('sku'), "{$read} must return only the selected attribute of {$product->getId()}");
                $this->assertFalse($product->offsetExists('store'), "{$read} must not return the back-reference of {$product->getId()}");
            }
        }

        $store = $database->getDocument('store', 'store1', [Query::select(['*', 'products.'])]);
        $this->assertSame(['product1', 'product2'], $this->relatedIds($store, 'products'));
        foreach ($store->getDocuments('products') as $product) {
            $this->assertSame("sku-{$product->getId()}", $product->getAttribute('sku'), 'A trailing dot selects every attribute of the related documents');
            $this->assertSame("Name {$product->getId()}", $product->getAttribute('name'));
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRelationshipFilterConversionEdgeCases(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $database->createCollection(Collection::create(id: 'project', permissions: $this->permissions(), documentSecurity: false));
        $database->createCollection(Collection::create(id: 'developer', attributes: [Attribute::string(key: 'devName', size: 64)], permissions: $this->permissions(), documentSecurity: false));
        $database->createRelationship('project', Relationship::manyToMany(relatedCollection: 'developer', twoWay: true, key: 'developers', twoWayKey: 'projects', onDelete: RelationshipDeleteAction::SetNull));

        foreach (['dev1' => 'Alice', 'dev2' => 'Bob', 'dev3' => 'Carol'] as $id => $name) {
            $database->createDocument('developer', new Document(['$id' => $id, 'devName' => $name]));
        }
        $database->createDocument('project', new Document(['$id' => 'project1', 'developers' => ['dev1', 'dev2']]));
        $database->createDocument('project', new Document(['$id' => 'project2', 'developers' => ['dev1', 'dev3']]));

        $projects = function (Query $query) use ($database): array {
            $ids = \array_map(fn (Document $project): string => $project->getId(), $database->find('project', [$query]));
            \sort($ids);

            return $ids;
        };

        $this->assertSame(['project1'], $projects(Query::containsAll('developers.$id', ['dev2'])));
        $this->assertSame(['project2'], $projects(Query::containsAll('developers.$id', ['dev1', 'dev3'])));
        $this->assertSame([], $projects(Query::containsAll('developers.$id', ['dev1', 'nobody'])), 'A value no related document matches leaves no project');
        $this->assertSame([], $projects(Query::containsAll('developers.$id', ['dev2', 'dev3'])), 'Values no single project holds together leave no project');
        $this->assertSame([], $projects(Query::equal('developers.devName', ['Nobody'])));

        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            return;
        }

        try {
            $database->find('project', [Query::equal('developers.unknownAttribute', ['x'])]);
            $this->fail('A filter on an unknown related attribute must be rejected');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('unknownAttribute', $exception->getMessage());
        }
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, Relationship}>
     */
    public static function manySideRelationships(): iterable
    {
        foreach (self::adapters() as $adapterName => [$adapter]) {
            yield "{$adapterName}: one-to-many" => [$adapter, Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull)];
            yield "{$adapterName}: many-to-many" => [$adapter, Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull)];
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('manySideRelationships')]
    public function testSetOperatorsDecideWhichDocumentsStayLinked(Closure $adapter, Relationship $relationship): void
    {
        $database = $this->database($adapter);
        $this->relate($database, $relationship);
        foreach (['child1', 'child2', 'child3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id]));
        }
        $database->createDocument('parent', new Document(['$id' => 'parent1', 'children' => ['child1', 'child2', 'child3']]));

        $steps = [
            'arrayUnique' => [Operator::arrayUnique(), ['child1', 'child2', 'child3']],
            'arrayFilter' => [Operator::arrayFilter('isNotNull'), ['child1', 'child2', 'child3']],
            'arrayIntersect' => [Operator::arrayIntersect(['child1', 'child2']), ['child1', 'child2']],
            'arrayDiff' => [Operator::arrayDiff(['child1']), ['child2']],
            'arrayInsert' => [Operator::arrayInsert(0, 'child3'), ['child2', 'child3']],
        ];
        foreach ($steps as $step => [$operator, $expected]) {
            $database->updateDocument('parent', 'parent1', new Document(['children' => $operator]));

            $this->assertSame($expected, $this->relatedIds($database->getDocument('parent', 'parent1'), 'children'), "After {$step}");
            foreach (['child1', 'child2', 'child3'] as $id) {
                $linked = $relationship->type === RelationshipType::OneToMany
                    ? $database->getDocument('child', $id)->getDocument('parent')->getId() === 'parent1'
                    : $this->relatedIds($database->getDocument('child', $id), 'parents') === ['parent1'];
                $this->assertSame(\in_array($id, $expected, true), $linked, "After {$step}, {$id} seen from its own side");
            }
        }
    }

    public function testLinkingAChildGrantedUpdateOnlyByItsOwnPermissionsWithoutThePermissionsHook(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('relationship_hook')
            ->setNamespace('relationship_hook_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());

        $this->relate(
            $database,
            Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull),
            [Permission::create(Role::any()), Permission::read(Role::any())],
        );

        $database->createDocument('parent', new Document(['$id' => 'parent1']));
        $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]]));

        $database->updateDocument('parent', 'parent1', new Document(['children' => ['child1']]));

        $child = $database->skipRelationships(fn () => $database->getDocument('child', 'child1'));
        $this->assertSame('parent1', $child->getAttribute('parent'), 'A child the caller may update through its own permissions must be linked');
    }
}
