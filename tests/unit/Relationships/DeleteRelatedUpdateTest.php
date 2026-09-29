<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Event\FailingLifecycle;
use Tests\Unit\Event\RecordingLifecycle;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ForeignKeyAction;

/**
 * deleteDocument() fires Event::DocumentUpdate for every document on the other side of a
 * two-way relationship that the delete changed, including the ones it never writes to.
 */
final class DeleteRelatedUpdateTest extends TestCase
{
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
    public function testDeletingAParentReportsEachChildAsTheSetNullWroteIt(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2']);

        $recorder = $this->record($database);
        $this->assertTrue($database->deleteDocument('parent', 'parent1'));

        $this->assertSame(['child1', 'child2'], $this->reported($recorder));
        foreach ($recorder->getPayloads(Event::DocumentUpdate) as $related) {
            $this->assertInstanceOf(Document::class, $related);
            $this->assertSame('child', $related->getCollection());
            $this->assertTrue(\array_key_exists('parent', $related->getArrayCopy()));
            $this->assertNull($related->getAttribute('parent'));
            $this->assertSame($database->getDocument('child', $related->getId())->getUpdatedAt(), $related->getUpdatedAt());
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingAChildReportsTheParentItNeverWrote(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2']);

        $recorder = $this->record($database);
        $database->deleteDocument('child', 'child1');

        $this->assertSame(['parent1'], $this->reported($recorder));
        $related = $recorder->getPayloads(Event::DocumentUpdate)[0];
        $this->assertInstanceOf(Document::class, $related);
        $this->assertSame('parent', $related->getCollection());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testACascadeReportsOnlyThePeersItDidNotRemove(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::Cascade);
        $database->createCollection(new Collection(id: 'owner', permissions: $this->collectionPermissions(), documentSecurity: true));
        $database->createRelationship(Relationship::manyToOne(collection: 'parent', relatedCollection: 'owner', twoWay: true, key: 'owner', twoWayKey: 'owned', onDelete: ForeignKeyAction::SetNull));
        $database->createDocument('owner', new Document(['$id' => 'owner1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'children' => ['child1'], 'owner' => 'owner1']));

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['owner1'], $this->reported($recorder));
        $this->assertTrue($database->getDocument('child', 'child1')->isEmpty());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingAChildUnderRestrictReportsItsParent(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::Restrict);
        $this->createFamily($database, 'parent1', ['child1']);

        $recorder = $this->record($database);
        $database->deleteDocument('child', 'child1');

        $this->assertSame(['parent1'], $this->reported($recorder));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingAManyToManySideReportsThePeers(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->createCollections($database, 'parent', 'child');
        $database->createRelationship(Relationship::manyToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: ForeignKeyAction::SetNull));
        $this->createFamily($database, 'parent1', ['child1', 'child2']);

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['child1', 'child2'], $this->reported($recorder));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingAOneToOneSideReportsItsPartner(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->createCollections($database, 'parent', 'child');
        $database->createRelationship(Relationship::oneToOne(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'partner', twoWayKey: 'partnerOf', onDelete: ForeignKeyAction::SetNull));
        $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'partner' => 'child1']));

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['child1'], $this->reported($recorder));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testOneWayPeersAreNotReportedWhileTwoWayPeersAre(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->createCollections($database, 'parent', 'stray', 'child');
        $database->createRelationship(Relationship::oneToMany(collection: 'parent', relatedCollection: 'stray', key: 'strays', onDelete: ForeignKeyAction::SetNull));
        $database->createRelationship(Relationship::manyToOne(collection: 'parent', relatedCollection: 'stray', key: 'stray', twoWayKey: 'strayOf', onDelete: ForeignKeyAction::SetNull));
        $database->createDocument('stray', new Document(['$id' => 'stray1', '$permissions' => $this->documentPermissions()]));
        $database->createRelationship(Relationship::oneToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: ForeignKeyAction::SetNull));
        $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'strays' => ['stray1'], 'stray' => 'stray1', 'children' => ['child1']]));

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['child1'], $this->reported($recorder));
        $this->assertFalse($database->getDocument('stray', 'stray1')->isEmpty());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAPeerCascadedAwayByAnotherRelationshipIsNotReportedWhileASurvivorIs(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $database->createCollection(new Collection(id: 'pair', permissions: $this->collectionPermissions(), documentSecurity: true));
        $database->createRelationship(Relationship::manyToOne(collection: 'parent', relatedCollection: 'pair', twoWay: true, key: 'owner', twoWayKey: 'owned', onDelete: ForeignKeyAction::SetNull));
        $database->createRelationship(Relationship::oneToOne(collection: 'parent', relatedCollection: 'pair', twoWay: true, key: 'buddy', twoWayKey: 'buddyOf', onDelete: ForeignKeyAction::Cascade));
        $database->createDocument('pair', new Document(['$id' => 'pair1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'owner' => 'pair1', 'buddy' => 'pair1', 'children' => ['child1']]));

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['child1'], $this->reported($recorder));
        $this->assertTrue($database->getDocument('pair', 'pair1')->isEmpty());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAPeerRemovedDownACascadeChainIsNotReportedWhileItsSiblingIs(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $database->createCollection(new Collection(id: 'pair', permissions: $this->collectionPermissions(), documentSecurity: true));
        $database->createRelationship(Relationship::oneToOne(collection: 'parent', relatedCollection: 'pair', twoWay: true, key: 'buddy', twoWayKey: 'buddyOf', onDelete: ForeignKeyAction::Cascade));
        $database->createRelationship(Relationship::oneToOne(collection: 'pair', relatedCollection: 'child', twoWay: true, key: 'tail', twoWayKey: 'tailOf', onDelete: ForeignKeyAction::Cascade));
        foreach (['child1', 'child2'] as $childId) {
            $database->createDocument('child', new Document(['$id' => $childId, '$permissions' => $this->documentPermissions()]));
        }
        $database->createDocument('pair', new Document(['$id' => 'pair1', '$permissions' => $this->documentPermissions(), 'tail' => 'child1']));
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'children' => ['child1', 'child2'], 'buddy' => 'pair1']));

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['child2'], $this->reported($recorder));
        $this->assertTrue($database->getDocument('child', 'child1')->isEmpty());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRelatedUpdatesFireAfterTheDelete(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1']);

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame([Event::DocumentPurge, Event::DocumentDelete, Event::DocumentUpdate], $recorder->getEvents());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testASilentDeleteReportsNothingWhileAHeardOneDoes(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1']);
        $this->createFamily($database, 'parent2', ['child2']);

        $recorder = $this->record($database);
        $database->silent(fn (): bool => $database->deleteDocument('parent', 'parent1'));

        $this->assertSame([], $recorder->getEvents());
        $this->assertNull($database->getDocument('child', 'child1')->getAttribute('parent'));

        $database->deleteDocument('parent', 'parent2');

        $this->assertSame(['child2'], $this->reported($recorder));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testABulkDeleteReportsNoRelatedUpdatesWhileASingleDeleteDoes(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1']);
        $this->createFamily($database, 'parent2', ['child2']);

        $recorder = $this->record($database);
        $this->assertSame(1, $database->deleteDocuments('parent', [Query::equal(Document::ID, ['parent1'])]));

        $this->assertSame([], $this->reported($recorder));

        $database->deleteDocument('parent', 'parent2');

        $this->assertSame(['child2'], $this->reported($recorder));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testOneFailingReportDoesNotCostTheOthersTheirs(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2']);

        $recorder = $this->record($database);
        $failure = new RuntimeException('related update hook failed');
        $database->addHook(new FailingLifecycle(Event::DocumentUpdate, $failure));

        try {
            $database->deleteDocument('parent', 'parent1');
            $this->fail('The failing hook must reach the caller');
        } catch (RuntimeException $caught) {
            $this->assertSame($failure, $caught);
        }

        $this->assertSame(['child1', 'child2'], $this->reported($recorder));
        $this->assertTrue($database->getDocument('parent', 'parent1')->isEmpty());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAFailingDeleteHookDoesNotCostTheRelatedUpdates(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1']);

        $failure = new RuntimeException('delete hook failed');
        $database->addHook(new FailingLifecycle(Event::DocumentDelete, $failure));
        $recorder = $this->record($database);

        try {
            $database->deleteDocument('parent', 'parent1');
            $this->fail('The failing hook must reach the caller');
        } catch (RuntimeException $caught) {
            $this->assertSame($failure, $caught);
        }

        $this->assertSame(['child1'], $this->reported($recorder));
    }

    public function testACommitThatFailsAndRetriesReportsEachPeerOnce(): void
    {
        $adapter = new class () extends Memory {
            public bool $failNextCommit = false;

            public function commitTransaction(): bool
            {
                if ($this->failNextCommit && $this->inTransaction === 1) {
                    $this->failNextCommit = false;

                    throw new RuntimeException('commit failed');
                }

                return parent::commitTransaction();
            }
        };

        $database = $this->database($adapter);
        $this->relateParentToChildren($database, ForeignKeyAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2']);

        $recorder = $this->record($database);
        $adapter->failNextCommit = true;
        $this->assertTrue($database->deleteDocument('parent', 'parent1'));

        $this->assertFalse($adapter->failNextCommit);
        $this->assertSame(['child1', 'child2'], $this->reported($recorder));
    }

    private function database(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('related_update')
            ->setNamespace('related_update_'.\uniqid());

        $database->create();
        $database->addHook(new Relationships($database));
        $database->addHook(new Permissions());

        return $database;
    }

    private function relateParentToChildren(Database $database, ForeignKeyAction $onDelete): void
    {
        $this->createCollections($database, 'parent', 'child');
        $database->createRelationship(Relationship::oneToMany(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: $onDelete));
    }

    private function createCollections(Database $database, string ...$ids): void
    {
        foreach ($ids as $id) {
            $database->createCollection(new Collection(id: $id, permissions: $this->collectionPermissions(), documentSecurity: true));
        }
    }

    /**
     * @param  list<string>  $children
     */
    private function createFamily(Database $database, string $parent, array $children): void
    {
        foreach ($children as $child) {
            $database->createDocument('child', new Document(['$id' => $child, '$permissions' => $this->documentPermissions()]));
        }

        $database->createDocument('parent', new Document(['$id' => $parent, '$permissions' => $this->documentPermissions(), 'children' => $children]));
    }

    private function record(Database $database): RecordingLifecycle
    {
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        return $recorder;
    }

    /**
     * @return list<string>
     */
    private function reported(RecordingLifecycle $recorder): array
    {
        $ids = [];
        foreach ($recorder->getPayloads(Event::DocumentUpdate) as $related) {
            $this->assertInstanceOf(Document::class, $related);
            $ids[] = $related->getId();
        }
        \sort($ids);

        return $ids;
    }

    /**
     * @return list<string>
     */
    private function collectionPermissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    /**
     * @return list<string>
     */
    private function documentPermissions(): array
    {
        return [
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }
}
