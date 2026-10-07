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
use Utopia\Database\Event\DispatcherHook;
use Utopia\Database\Event\Document\Deleted as DocumentDeleted;
use Utopia\Database\Event\Document\Updated as DocumentUpdated;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Profiler\QueryLog;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Validator\Authorization;

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
     * Adapters that hand every bulk update to $watch, along with the write itself.
     *
     * @return iterable<string, array{Closure(Closure(Document, array<Document>, Closure(): int): int): Adapter}>
     */
    public static function watchedAdapters(): iterable
    {
        yield 'memory' => [static fn (Closure $watch): Adapter => new class ($watch) extends Memory {
            /**
             * @param  Closure(Document, array<Document>, Closure(): int): int  $watch
             */
            public function __construct(private readonly Closure $watch)
            {
                parent::__construct();
            }

            #[\Override]
            public function updateDocuments(Document $collection, Document $updates, array $documents): int
            {
                return ($this->watch)($collection, $documents, fn (): int => parent::updateDocuments($collection, $updates, $documents));
            }
        }];
        yield 'sqlite' => [static fn (Closure $watch): Adapter => new class (new PDO('sqlite::memory:'), $watch) extends SQLite {
            /**
             * @param  Closure(Document, array<Document>, Closure(): int): int  $watch
             */
            public function __construct(PDO $pdo, private readonly Closure $watch)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            public function updateDocuments(Document $collection, Document $updates, array $documents): int
            {
                return ($this->watch)($collection, $documents, fn (): int => parent::updateDocuments($collection, $updates, $documents));
            }
        }];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingAParentReportsEachChildAsTheSetNullWroteIt(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::Cascade);
        $database->createCollection(Collection::create(id: 'owner', permissions: $this->collectionPermissions(), documentSecurity: true));
        $database->createRelationship('parent', Relationship::manyToOne(relatedCollection: 'owner', twoWay: true, key: 'owner', twoWayKey: 'owned', onDelete: RelationshipDeleteAction::SetNull));
        $database->createDocument('owner', new Document(['$id' => 'owner1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => $this->documentPermissions()]));
        $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'children' => ['child1'], 'owner' => 'owner1']));

        $recorder = $this->record($database);
        $database->deleteDocument('parent', 'parent1');

        $this->assertSame(['owner1'], $this->reported($recorder));
        $this->assertTrue($database->getDocument('child', 'child1')->isEmpty());
    }

    public function testACascadeReadsNoPeersWhenNoHookListensForRelatedUpdates(): void
    {
        $reads = [];
        foreach (['no lifecycle hook' => null, 'a hook for deletes only' => DocumentDeleted::class, 'a hook for updates' => DocumentUpdated::class] as $case => $listened) {
            $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
            $this->relateParentToChildren($database, RelationshipDeleteAction::Cascade);
            $database->createCollection(Collection::create(id: 'owner', permissions: $this->collectionPermissions(), documentSecurity: true));
            $database->createRelationship('parent', Relationship::manyToOne(relatedCollection: 'owner', twoWay: true, key: 'owner', twoWayKey: 'owned', onDelete: RelationshipDeleteAction::SetNull));
            $database->createDocument('owner', new Document(['$id' => 'owner1', '$permissions' => $this->documentPermissions()]));
            $database->createDocument('child', new Document(['$id' => 'child1', '$permissions' => $this->documentPermissions()]));
            $database->createDocument('parent', new Document(['$id' => 'parent1', '$permissions' => $this->documentPermissions(), 'children' => ['child1'], 'owner' => 'owner1']));

            $heard = [];
            if ($listened !== null) {
                $dispatcher = new DispatcherHook();
                $dispatcher->on($listened, function (DocumentDeleted|DocumentUpdated $event) use (&$heard): void {
                    $heard[] = $event instanceof DocumentUpdated ? $event->document->getId() : $event->documentId;
                });
                $database->addHook($dispatcher);
            }

            $database->enableProfiling();
            $this->assertTrue($database->deleteDocument('parent', 'parent1'), $case);
            $reads[$case] = \count(\array_filter(
                $database->getProfiler()?->getLogs() ?? [],
                static fn (QueryLog $log): bool => \str_starts_with(\ltrim($log->query), 'SELECT'),
            ));

            $this->assertSame(match ($listened) {
                DocumentDeleted::class => ['parent1'],
                DocumentUpdated::class => ['owner1'],
                default => [],
            }, $heard, $case);
        }

        $this->assertSame($reads['no lifecycle hook'], $reads['a hook for deletes only'], 'A hook that does not listen for related updates must not cost the delete the reads that find them');
        $this->assertGreaterThan($reads['no lifecycle hook'], $reads['a hook for updates'], 'A hook that listens for them still gets the peers the cascade left');
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingAChildUnderRestrictReportsItsParent(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $this->relateParentToChildren($database, RelationshipDeleteAction::Restrict);
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
        $database->createRelationship('parent', Relationship::manyToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parents', onDelete: RelationshipDeleteAction::SetNull));
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
        $database->createRelationship('parent', Relationship::oneToOne(relatedCollection: 'child', twoWay: true, key: 'partner', twoWayKey: 'partnerOf', onDelete: RelationshipDeleteAction::SetNull));
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
        $database->createRelationship('parent', Relationship::oneToMany(relatedCollection: 'stray', key: 'strays', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('parent', Relationship::manyToOne(relatedCollection: 'stray', key: 'stray', twoWayKey: 'strayOf', onDelete: RelationshipDeleteAction::SetNull));
        $database->createDocument('stray', new Document(['$id' => 'stray1', '$permissions' => $this->documentPermissions()]));
        $database->createRelationship('parent', Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: RelationshipDeleteAction::SetNull));
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
        $database->createCollection(Collection::create(id: 'pair', permissions: $this->collectionPermissions(), documentSecurity: true));
        $database->createRelationship('parent', Relationship::manyToOne(relatedCollection: 'pair', twoWay: true, key: 'owner', twoWayKey: 'owned', onDelete: RelationshipDeleteAction::SetNull));
        $database->createRelationship('parent', Relationship::oneToOne(relatedCollection: 'pair', twoWay: true, key: 'buddy', twoWayKey: 'buddyOf', onDelete: RelationshipDeleteAction::Cascade));
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
        $database->createCollection(Collection::create(id: 'pair', permissions: $this->collectionPermissions(), documentSecurity: true));
        $database->createRelationship('parent', Relationship::oneToOne(relatedCollection: 'pair', twoWay: true, key: 'buddy', twoWayKey: 'buddyOf', onDelete: RelationshipDeleteAction::Cascade));
        $database->createRelationship('pair', Relationship::oneToOne(relatedCollection: 'child', twoWay: true, key: 'tail', twoWayKey: 'tailOf', onDelete: RelationshipDeleteAction::Cascade));
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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

    /**
     * @param  Closure(Closure(Document, array<Document>, Closure(): int): int): Adapter  $adapter
     */
    #[DataProvider('watchedAdapters')]
    public function testASetNullDeleteNobodyHearsKeepsNoEarlierChunkOfPeers(Closure $adapter): void
    {
        $peers = new DeleteRelatedUpdateRetention('child');
        $database = $this->database($adapter($peers->watch(...)));
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2', 'child3']);
        $dispatcher = new DispatcherHook();
        $deleted = [];
        $dispatcher->on(DocumentDeleted::class, static function (DocumentDeleted $event) use (&$deleted): void {
            $deleted[] = $event->documentId;
        });
        $database->addHook($dispatcher);
        $database->setMaxQueryValues(1);
        $this->assertSame(['child1', 'child2', 'child3'], $this->childIds($database, 'parent1'));

        $peers->reset();
        $this->assertTrue($database->deleteDocument('parent', 'parent1'));

        $this->assertSame(3, $peers->written, 'Every child must be cleared');
        $this->assertSame(0, $peers->mostAlive, 'A delete nobody hears must let each chunk of cleared children go before writing the next');
        $this->assertSame(['parent1'], $deleted);
        foreach (['child1', 'child2', 'child3'] as $childId) {
            $this->assertNull($database->getDocument('child', $childId)->getAttribute('parent'), $childId);
        }
    }

    /**
     * @param  Closure(Closure(Document, array<Document>, Closure(): int): int): Adapter  $adapter
     */
    #[DataProvider('watchedAdapters')]
    public function testASetNullBulkDeleteKeepsNoEarlierChunkOfPeers(Closure $adapter): void
    {
        $peers = new DeleteRelatedUpdateRetention('child');
        $database = $this->database($adapter($peers->watch(...)));
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2', 'child3']);
        $this->record($database);
        $database->setMaxQueryValues(1);

        $peers->reset();
        $this->assertSame(1, $database->deleteDocuments('parent', [Query::equal(Document::ID, ['parent1'])]));

        $this->assertSame(3, $peers->written, 'Every child must be cleared');
        $this->assertSame(0, $peers->mostAlive, 'A bulk delete reports no related updates, so it must let each chunk of cleared children go');
        foreach (['child1', 'child2', 'child3'] as $childId) {
            $this->assertNull($database->getDocument('child', $childId)->getAttribute('parent'), $childId);
        }
    }

    /**
     * @param  Closure(Closure(Document, array<Document>, Closure(): int): int): Adapter  $adapter
     */
    #[DataProvider('watchedAdapters')]
    public function testASetNullDeleteSomeoneHearsReportsEveryChunksPeers(Closure $adapter): void
    {
        $peers = new DeleteRelatedUpdateRetention('child');
        $database = $this->database($adapter($peers->watch(...)));
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
        $this->createFamily($database, 'parent1', ['child1', 'child2', 'child3']);
        $recorder = $this->record($database);
        $database->setMaxQueryValues(1);

        $peers->reset();
        $this->assertTrue($database->deleteDocument('parent', 'parent1'));

        $this->assertSame(3, $peers->written, 'Every child must be cleared');
        $this->assertSame(['child1', 'child2', 'child3'], $this->reported($recorder));
        foreach ($recorder->getPayloads(Event::DocumentUpdate) as $related) {
            $this->assertInstanceOf(Document::class, $related);
            $this->assertNull($related->getAttribute('parent'));
        }
    }

    public function testACommitThatFailsAndRetriesReportsEachPeerOnce(): void
    {
        $adapter = new class () extends Memory {
            public bool $failNextCommit = false;

            public function commitTransaction(): bool
            {
                if ($this->failNextCommit && $this->inTransaction === 1) {
                    $this->failNextCommit = false;

                    throw new TransactionException('Failed to commit transaction: commit failed');
                }

                return parent::commitTransaction();
            }
        };

        $database = $this->database($adapter);
        $this->relateParentToChildren($database, RelationshipDeleteAction::SetNull);
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

    private function relateParentToChildren(Database $database, RelationshipDeleteAction $onDelete): void
    {
        $this->createCollections($database, 'parent', 'child');
        $database->createRelationship('parent', Relationship::oneToMany(relatedCollection: 'child', twoWay: true, key: 'children', twoWayKey: 'parent', onDelete: $onDelete));
    }

    private function createCollections(Database $database, string ...$ids): void
    {
        foreach ($ids as $id) {
            $database->createCollection(Collection::create(id: $id, permissions: $this->collectionPermissions(), documentSecurity: true));
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

    /**
     * @return list<string>
     */
    private function childIds(Database $database, string $parent): array
    {
        $children = $database->getDocument('parent', $parent)->getAttribute('children', []);
        $this->assertIsArray($children);

        $ids = [];
        foreach ($children as $child) {
            $this->assertInstanceOf(Document::class, $child);
            $ids[] = $child->getId();
        }
        \sort($ids);

        return $ids;
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
