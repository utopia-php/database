<?php

namespace Tests\Unit;

use Closure;
use DateTime;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Mirror;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

use function Swoole\Coroutine\run;

final class MirrorReplicationTest extends TestCase
{
    private const string NOTES = 'notes';

    private const string SECRETS = 'secrets';

    private const string PARENTS = 'parents';

    private const string CHILDREN = 'children';

    public const string DELETED = 'deleted';

    public const string LOCKED = 'read for update';

    public const string INCREASED = 'increased';

    private Authorization $authorization;

    private Mirror $mirror;

    private Database $destination;

    /**
     * Seconds each destination write waits before it runs, by the title it writes.
     *
     * @var array<string, float>
     */
    private array $delays = [];

    /**
     * Destination writes in the order they completed, as [document id, title written, coroutine id].
     *
     * @var list<array{string, string, int}>
     */
    private array $writes = [];

    /**
     * @var list<array{string, string}>
     */
    private array $errors = [];

    /**
     * Destination adapter calls in progress, and the most that were ever in progress at once.
     */
    private int $busy = 0;

    private int $peak = 0;

    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for asynchronous replication');
        }

        $this->authorization = new Authorization();
        $this->destination = new Database($this->yieldingAdapter(), new Cache(new None()));
        $this->mirror = new Mirror(new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None())), $this->destination);
        $this->mirror
            ->setAuthorization($this->authorization)
            ->setDatabase('mirror')
            ->setNamespace('replication_'.\uniqid())
            ->create();
        $this->mirror->onError(function (string $action, Throwable $error): void {
            $this->errors[] = [$action, $error->getMessage()];
        });

        $this->authorization->skip(function (): void {
            $this->mirror->createCollection(new Collection(
                id: self::NOTES,
                attributes: [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'views')],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                documentSecurity: false,
            ));
            $this->mirror->createCollection(new Collection(
                id: self::SECRETS,
                attributes: [Attribute::string(key: 'title', size: 64)],
                permissions: [Permission::create(Role::any())],
                documentSecurity: true,
            ));
            $this->mirror->createDocument(self::NOTES, new Document([Document::ID => 'public', 'title' => 'public']));
            $this->mirror->createDocument(self::SECRETS, new Document([
                Document::ID => 'secret',
                'title' => 'secret',
                Document::PERMISSIONS => [Permission::read(Role::user('alice'))],
            ]));
        });

        $this->writes = [];
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::any()->toString());
    }

    public function testAReplicatedWriteLeavesTheCallersAuthorizationUnchanged(): void
    {
        $seen = null;
        $status = null;

        $this->inCoroutine(function () use (&$seen, &$status): void {
            $this->mirror->deleteDocument(self::NOTES, 'public');
            $seen = $this->mirror->find(self::SECRETS);
            $status = $this->authorization->getStatus();
        });

        $this->assertSame([], $seen, 'A guest must not read a document only alice may read');
        $this->assertTrue($status);
        $this->assertTrue($this->authorization->getStatus());
        $this->assertSame([], $this->errors);
        $this->assertSame([['public', self::DELETED]], $this->titlesWritten());
    }

    public function testReplicationRunsUnderTheCallersStateAfterTheCallerLeftItsScope(): void
    {
        $this->inCoroutine(function (): void {
            $this->authorization->skip(fn (): bool => $this->mirror->deleteDocument(self::SECRETS, 'secret'));
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['secret', self::DELETED]], $this->titlesWritten());
        $this->assertTrue($this->authorization->skip(fn (): bool => $this->destination->getDocument(self::SECRETS, 'secret')->isEmpty()));
    }

    public function testReplicationOutsideACoroutineIsSynchronous(): void
    {
        $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'v0'])]);
        $this->mirror->deleteDocument(self::NOTES, 'public');

        $this->assertSame([['first', 'v0'], ['public', self::DELETED]], $this->titlesWritten());
        $this->assertSame([-1], \array_values(\array_unique(\array_column($this->writes, 2))), 'Every destination call runs in the caller');
        $this->assertSame('v0', $this->destination->getDocument(self::NOTES, 'first')->getAttribute('title'));
        $this->assertTrue($this->destination->getDocument(self::NOTES, 'public')->isEmpty());
    }

    /**
     * Delays that make later writes finish first when nothing orders them.
     *
     * @return iterable<string, array{array<string, float>}>
     */
    public static function interleavings(): iterable
    {
        yield 'latest first' => [['v0' => 0.05, 'v1' => 0.04, 'v2' => 0.03, 'v3' => 0.02, self::DELETED => 0.01]];
        yield 'mixed' => [['v0' => 0.02, 'v1' => 0.05, 'v2' => 0.01, 'v3' => 0.04, self::DELETED => 0.03]];
        yield 'create last' => [['v0' => 0.04, 'v1' => 0.01, 'v2' => 0.05, 'v3' => 0.02, self::DELETED => 0.03]];
    }

    /**
     * @param  array<string, float>  $delays
     */
    #[DataProvider('interleavings')]
    public function testWritesToOneDocumentReachTheDestinationInOrder(array $delays): void
    {
        $this->delays = $delays;

        foreach (['kept' => false, 'removed' => true] as $id => $delete) {
            $this->inCoroutine(function () use ($id, $delete): void {
                $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => $id, 'title' => 'v0'])]);
                $this->mirror->updateDocuments(self::NOTES, new Document(['title' => 'v1']), [Query::equal(Document::ID, [$id])]);
                $this->mirror->updateDocument(self::NOTES, $id, new Document(['title' => 'v2']));
                $this->mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => $id, 'title' => 'v3'])]);
                if ($delete) {
                    $this->mirror->deleteDocument(self::NOTES, $id);
                }
            });
        }

        $this->assertSame([], $this->errors);
        $this->assertSame([
            ['kept', 'v0'], ['kept', 'v1'], ['kept', 'v2'], ['kept', 'v3'],
            ['removed', 'v0'], ['removed', 'v1'], ['removed', 'v2'], ['removed', 'v3'], ['removed', self::DELETED],
        ], $this->titlesWritten());
        $this->assertSame('v3', $this->destination->getDocument(self::NOTES, 'kept')->getAttribute('title'));
        $this->assertTrue($this->destination->getDocument(self::NOTES, 'removed')->isEmpty());
    }

    public function testAFailedReplicationIsReportedAndDoesNotHoldBackLaterWritesToTheDocument(): void
    {
        $this->delays = ['v0' => 0.03, 'broken' => 0.02, 'v2' => 0.01];

        $this->inCoroutine(function (): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'v0'])]);
            $this->mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'broken'])]);
            $this->mirror->updateDocuments(self::NOTES, new Document(['title' => 'v2']), [Query::equal(Document::ID, ['first'])]);
        });

        $this->assertSame([['upsertDocuments', 'destination rejected broken']], $this->errors);
        $this->assertSame([['first', 'v0'], ['first', 'v2']], $this->titlesWritten());
        $this->assertSame('v2', $this->destination->getDocument(self::NOTES, 'first')->getAttribute('title'));
    }

    public function testARecreatedDocumentWaitsForItsEarlierDeletion(): void
    {
        $this->delays = [self::DELETED => 0.03];

        $this->inCoroutine(function (): void {
            $this->mirror->deleteDocument(self::NOTES, 'public');
            $this->mirror->createDocument(self::NOTES, new Document([Document::ID => 'public', 'title' => 'v1']));
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['public', self::DELETED], ['public', 'v1']], $this->titlesWritten());
        $this->assertSame('v1', $this->destination->getDocument(self::NOTES, 'public')->getAttribute('title'));
    }

    public function testACounterUpdateWaitsForEarlierReplicationsOfTheDocument(): void
    {
        $this->delays = ['v0' => 0.03];

        $this->inCoroutine(function (): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'v0', 'views' => 1])]);
            $this->mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'v1', 'views' => 5])]);
            $this->mirror->increaseDocumentAttribute(self::NOTES, 'first', 'views', 2);
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['first', 'v0'], ['first', 'v1'], ['first', self::LOCKED], ['first', self::INCREASED]], $this->titlesWrittenAndLocked());
        $this->assertSame(7, $this->destination->getDocument(self::NOTES, 'first')->getAttribute('views'));
    }

    public function testReplicationsUseTheDestinationOneAtATimeInTheOrderTheyWereMade(): void
    {
        $this->delays = ['slow' => 0.05, 'fast' => 0.01];
        $this->peak = 0;

        $this->inCoroutine(function (): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'slow'])]);
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'second', 'title' => 'fast'])]);
            $this->mirror->updateDocument(self::NOTES, 'public', new Document(['title' => 'synchronous']));
            $this->mirror->deleteDocument(self::NOTES, 'first');
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['first', 'slow'], ['second', 'fast'], ['public', 'synchronous'], ['first', self::DELETED]], $this->titlesWritten());
        $this->assertSame(1, $this->peak, 'Replications sharing one destination connection must not overlap');
    }

    public function testAwaitReplicationsReturnsOnceEveryQueuedReplicationReachedTheDestination(): void
    {
        $this->delays = ['queued' => 0.03, 'broken' => 0.01];
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'queued'])]);
            $this->mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'broken'])]);
            $seen['queued'] = [$this->titlesWritten(), $this->errors];
            $this->mirror->awaitReplications();
            $seen['awaited'] = [$this->titlesWritten(), $this->errors];
            $this->mirror->awaitReplications();
        });

        $this->assertSame([[], []], $seen['queued']);
        $this->assertSame([[['first', 'queued']], [['upsertDocuments', 'destination rejected broken']]], $seen['awaited']);
    }

    public function testAwaitReplicationsOutsideACoroutineReturnsAtOnce(): void
    {
        $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'v0'])]);
        $this->mirror->awaitReplications();

        $this->assertSame([['first', 'v0']], $this->titlesWritten());
    }

    public function testAWriteThroughTheMirrorFromOnErrorDoesNotWaitForTheReplicationThatReportedIt(): void
    {
        $this->mirror->onError(function (string $action): void {
            $this->mirror->createDocument(self::NOTES, new Document([Document::ID => 'reported', 'title' => $action]));
            $this->mirror->awaitReplications();
        });

        $this->inCoroutine(function (): void {
            $this->mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'broken'])]);
            $this->mirror->updateDocument(self::NOTES, 'public', new Document(['title' => 'after']));
        });

        $this->assertSame([['upsertDocuments', 'destination rejected broken']], $this->errors);
        $this->assertSame([['reported', 'upsertDocuments'], ['public', 'after']], $this->titlesWritten());
    }

    public function testADocumentWrittenThroughARelationshipIsNotOvertakenByALaterWriteToIt(): void
    {
        $this->relate();
        $this->delays = ['parent' => 0.03, 'nested' => 0.03];

        $this->inCoroutine(function (): void {
            $this->mirror->createDocuments(self::PARENTS, [new Document([
                Document::ID => 'parent',
                'title' => 'parent',
                'children' => [new Document([Document::ID => 'child', 'title' => 'nested'])],
            ])]);
            $this->mirror->updateDocument(self::CHILDREN, 'child', new Document(['title' => 'renamed']));
        });

        $this->assertSame([], $this->errors);
        $this->assertSame('renamed', $this->mirror->getSource()->getDocument(self::CHILDREN, 'child')->getAttribute('title'));
        $this->assertSame('renamed', $this->destination->getDocument(self::CHILDREN, 'child')->getAttribute('title'));
        $this->assertFalse($this->destination->getDocument(self::PARENTS, 'parent')->isEmpty());
    }

    /**
     * @return iterable<string, array{Closure(Mirror): mixed}>
     */
    public static function relationshipChanges(): iterable
    {
        yield 'deleteRelationship' => [static fn (Mirror $mirror): bool => $mirror->deleteRelationship(self::PARENTS, 'children')];
        yield 'updateRelationship' => [static fn (Mirror $mirror): bool => $mirror->updateRelationship(self::PARENTS, 'children', newTwoWayKey: 'owner')];
        yield 'deleteCollection' => [static fn (Mirror $mirror): bool => $mirror->deleteCollection(self::PARENTS)];
    }

    /**
     * @param  Closure(Mirror): mixed  $change
     */
    #[DataProvider('relationshipChanges')]
    public function testARelationshipChangeWaitsForTheQueuedReplicationsOfTheRelatedCollection(Closure $change): void
    {
        $this->relate();
        $this->authorization->skip(fn (): Document => $this->mirror->createDocument(self::PARENTS, new Document([Document::ID => 'parent', 'title' => 'parent'])));
        $this->writes = [];
        $this->delays = ['queued' => 0.03];

        $this->inCoroutine(function () use ($change): void {
            $this->mirror->createDocuments(self::CHILDREN, [new Document([Document::ID => 'child', 'title' => 'queued', 'parent' => 'parent'])]);
            $change($this->mirror);
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['child', 'queued']], $this->writesOf('child'));
        $this->assertSame('queued', $this->destination->getDocument(self::CHILDREN, 'child')->getAttribute('title'));
    }

    /**
     * A write the mirror replicates before returning, then a write to the same document from another coroutine.
     *
     * @return iterable<string, array{Closure(Mirror): mixed, Closure(Mirror): mixed, array<string, float>}>
     */
    public static function overtakingWrites(): iterable
    {
        yield 'createDocument, then deleteDocument' => [
            static fn (Mirror $mirror): Document => $mirror->createDocument(self::NOTES, new Document([Document::ID => 'raced', 'title' => 'slow'])),
            static fn (Mirror $mirror): bool => $mirror->deleteDocument(self::NOTES, 'raced'),
            ['slow' => 0.03],
        ];
        yield 'updateDocument, then upsertDocuments' => [
            static fn (Mirror $mirror): Document => $mirror->updateDocument(self::NOTES, 'public', new Document(['title' => 'slow'])),
            static fn (Mirror $mirror): int => $mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => 'public', 'title' => 'latest'])]),
            ['slow' => 0.03],
        ];
        yield 'increaseDocumentAttribute, then updateDocuments' => [
            static fn (Mirror $mirror): Document => $mirror->increaseDocumentAttribute(self::NOTES, 'public', 'views', 5),
            static fn (Mirror $mirror): int => $mirror->updateDocuments(self::NOTES, new Document(['views' => 1]), [Query::equal(Document::ID, ['public'])]),
            [self::LOCKED => 0.03],
        ];
    }

    /**
     * @param  Closure(Mirror): mixed  $synchronous
     * @param  Closure(Mirror): mixed  $later
     * @param  array<string, float>  $delays
     */
    #[DataProvider('overtakingWrites')]
    public function testALaterReplicationFromAnotherCoroutineDoesNotOvertakeASynchronousOne(Closure $synchronous, Closure $later, array $delays): void
    {
        $this->delays = $delays;

        $this->inCoroutine(function () use ($synchronous, $later): void {
            $writers = new Channel(2);
            Coroutine::create(function () use ($synchronous, $writers): void {
                $synchronous($this->mirror);
                $writers->push(true);
            });
            Coroutine::create(function () use ($later, $writers): void {
                Coroutine::sleep(0.01);
                $later($this->mirror);
                $writers->push(true);
            });
            $writers->pop();
            $writers->pop();
        });

        $this->assertSame([], $this->errors);
        foreach (['raced', 'public'] as $id) {
            $this->assertSame($this->stored($this->mirror->getSource(), $id), $this->stored($this->destination, $id), "The destination's {$id} matches the source's");
        }
    }

    public function testReplicationRunsUnderTheCallersRolesAfterTheCallerChangedThem(): void
    {
        $this->authorization->skip(fn (): Document => $this->mirror->createDocument(self::SECRETS, new Document([
            Document::ID => 'owned',
            'title' => 'owned',
            Document::PERMISSIONS => [Permission::read(Role::user('alice')), Permission::delete(Role::user('alice'))],
        ])));
        $this->writes = [];
        $this->delays = [self::DELETED => 0.02];

        $this->inCoroutine(function (): void {
            $this->authorization->addRole(Role::user('alice')->toString());
            $this->mirror->deleteDocument(self::SECRETS, 'owned');
            $this->authorization->cleanRoles();
            $this->authorization->addRole(Role::any()->toString());
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['owned', self::DELETED]], $this->titlesWritten());
        $this->assertSame([Role::any()->toString()], $this->authorization->getRoles());
    }

    public function testAReplicationDoesNotChangeTheCallersRoles(): void
    {
        $this->delays = [self::DELETED => 0.01];
        $seen = [];

        $this->inCoroutine(function () use (&$seen): void {
            $this->authorization->addRole(Role::user('bob')->toString());
            $this->mirror->deleteDocument(self::NOTES, 'public');
            $seen['whileQueued'] = $this->authorization->getRoles();
            Coroutine::sleep(0.03);
            $seen['afterReplication'] = $this->authorization->getRoles();
        });

        $roles = [Role::any()->toString(), Role::user('bob')->toString()];
        $this->assertSame([], $this->errors);
        $this->assertSame(['whileQueued' => $roles, 'afterReplication' => $roles], $seen);
    }

    public function testConcurrentReplicationsDoNotShareTheDestinationsSkipDuplicates(): void
    {
        $this->authorization->skip(fn (): Document => $this->destination->createDocument(self::NOTES, new Document([
            Document::ID => 'second',
            'title' => 'only on the destination',
        ])));
        $this->writes = [];
        $this->delays = ['skipping' => 0.03];

        $this->inCoroutine(function (): void {
            $this->mirror->skipDuplicates(fn (): int => $this->mirror->createDocuments(self::NOTES, [
                new Document([Document::ID => 'first', 'title' => 'skipping']),
            ]));
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'second', 'title' => 'duplicate'])]);
        });

        $this->assertCount(1, $this->errors);
        $this->assertSame('createDocuments', $this->errors[0][0]);
        $this->assertStringContainsString('already exists', $this->errors[0][1]);
        $this->assertSame([['first', 'skipping']], $this->titlesWritten());
        $this->assertSame('only on the destination', $this->destination->getDocument(self::NOTES, 'second')->getAttribute('title'));
    }

    public function testOverlappingReplicationsLeaveTheDestinationsPreserveDatesSetting(): void
    {
        $this->delays = ['early' => 0.01, 'late' => 0.03];

        $this->inCoroutine(function (): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'early'])]);
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'second', 'title' => 'late'])]);
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['first', 'early'], ['second', 'late']], $this->titlesWritten());
        $this->assertFalse($this->destination->getPreserveDates());
    }

    public function testAReplicationDoesNotRecheckTheCallersRequestTimestampOnTheDestination(): void
    {
        \usleep(5000);
        $requestTimestamp = new DateTime();
        \usleep(5000);
        $this->destination->updateDocument(self::NOTES, 'public', new Document(['views' => 2]));
        $this->writes = [];

        $this->inCoroutine(function () use ($requestTimestamp): void {
            $this->mirror->withRequestTimestamp(
                $requestTimestamp,
                fn (): int => $this->mirror->upsertDocuments(self::NOTES, [new Document([Document::ID => 'public', 'title' => 'v1'])]),
            );
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['public', 'v1']], $this->titlesWritten());
    }

    public function testSynchronousAndAsynchronousReplicationsUseTheCallersTenant(): void
    {
        $destination = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $mirror = new Mirror(new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None())), $destination);
        $mirror
            ->setAuthorization(new Authorization())
            ->setDatabase('mirror')
            ->setNamespace('tenants_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(1)
            ->create();
        $mirror->onError(function (string $action, Throwable $error): void {
            $this->errors[] = [$action, $error->getMessage()];
        });
        foreach ([1, 2] as $tenant) {
            $mirror->setTenant($tenant);
            $mirror->createCollection(new Collection(
                id: self::NOTES,
                attributes: [Attribute::string(key: 'title', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
        }
        $mirror->setTenant(1);

        $mirror->withTenant(2, fn (): Document => $mirror->createDocument(self::NOTES, new Document([Document::ID => 'synchronous', 'title' => 'synchronous'])));
        $this->inCoroutine(function () use ($mirror): void {
            $mirror->withTenant(2, fn (): int => $mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'asynchronous', 'title' => 'asynchronous'])]));
        });

        $idsUnder = static fn (int $tenant): array => \array_map(
            static fn (Document $document): string => $document->getId(),
            $destination->withTenant($tenant, static fn (): array => $destination->find(self::NOTES, [Query::orderAsc(Document::ID)])),
        );
        $this->assertSame([], $this->errors);
        $this->assertSame([1 => [], 2 => ['asynchronous', 'synchronous']], [1 => $idsUnder(1), 2 => $idsUnder(2)]);
        $this->assertSame(1, $destination->getTenant());
    }

    /**
     * @return iterable<string, array{Closure(Mirror): mixed}>
     */
    public static function schemaChanges(): iterable
    {
        yield 'deleteAttribute' => [static fn (Mirror $mirror): bool => $mirror->deleteAttribute(self::NOTES, 'views')];
        yield 'renameAttribute' => [static fn (Mirror $mirror): bool => $mirror->renameAttribute(self::NOTES, 'views', 'count')];
        yield 'deleteCollection' => [static fn (Mirror $mirror): bool => $mirror->deleteCollection(self::NOTES)];
        yield 'updateCollection' => [static fn (Mirror $mirror): Document => $mirror->updateCollection(self::NOTES, [Permission::create(Role::any())], false)];
        yield 'createIndex' => [static fn (Mirror $mirror): bool => $mirror->createIndex(self::NOTES, new Index(key: 'views_index', type: IndexType::Key, attributes: ['views']))];
        yield 'updateAttributeRequired' => [static fn (Mirror $mirror): Document => $mirror->updateAttributeRequired(self::NOTES, 'title', false)];
        yield 'createRelationship' => [static fn (Mirror $mirror): bool => $mirror->createRelationship(new Relationship(
            collection: self::SECRETS,
            relatedCollection: self::NOTES,
            type: RelationType::ManyToOne,
            key: 'note',
        ))];
    }

    /**
     * @param  Closure(Mirror): mixed  $change
     */
    #[DataProvider('schemaChanges')]
    public function testASchemaChangeWaitsForTheQueuedReplicationsOfItsCollection(Closure $change): void
    {
        $this->delays = ['v0' => 0.03];

        $this->inCoroutine(function () use ($change): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'v0', 'views' => 1])]);
            $change($this->mirror);
        });

        $this->assertSame([], $this->errors);
        $this->assertSame(['first', 'v0'], $this->titlesWritten()[0] ?? null, 'The queued write reaches the destination before the schema change');
    }

    public function testASchemaChangeWaitsForTheQueuedReplicationsOfEveryCollection(): void
    {
        $this->authorization->skip(fn (): bool => $this->mirror->createAttribute(self::SECRETS, Attribute::integer(key: 'extra')));
        $this->delays = ['slow' => 0.05];
        $writesBeforeTheChangeReturned = null;

        $this->inCoroutine(function () use (&$writesBeforeTheChangeReturned): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'slow'])]);
            $this->mirror->deleteAttribute(self::SECRETS, 'extra');
            $writesBeforeTheChangeReturned = $this->writesOf('first');
        });

        $this->assertSame([['first', 'slow']], $writesBeforeTheChangeReturned);
        $this->assertSame([], $this->errors);
    }

    /**
     * Parents with a two-way one-to-many relationship to children, created through the mirror.
     */
    private function relate(): void
    {
        $this->mirror->addHook(new Relationships($this->mirror));
        $this->authorization->skip(function (): void {
            foreach ([self::PARENTS, self::CHILDREN] as $collection) {
                $this->mirror->createCollection(new Collection(
                    id: $collection,
                    attributes: [Attribute::string(key: 'title', size: 64)],
                    permissions: [
                        Permission::create(Role::any()),
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    documentSecurity: false,
                ));
            }
            $this->mirror->createRelationship(new Relationship(
                collection: self::PARENTS,
                relatedCollection: self::CHILDREN,
                type: RelationType::OneToMany,
                twoWay: true,
                key: 'children',
                twoWayKey: 'parent',
            ));
        });
    }

    /**
     * @return array{mixed, mixed}|null The title and views a database stores for the note, or null when it has none
     */
    private function stored(Database $database, string $id): ?array
    {
        $note = $this->authorization->skip(static fn (): Document => $database->getDocument(self::NOTES, $id));

        return $note->isEmpty() ? null : [$note->getAttribute('title'), $note->getAttribute('views')];
    }

    /**
     * @return list<array{string, string}>
     */
    private function titlesWritten(): array
    {
        return \array_values(\array_filter(
            $this->titlesWrittenAndLocked(),
            static fn (array $write): bool => $write[1] !== self::LOCKED,
        ));
    }

    /**
     * @return list<array{string, string}>
     */
    private function writesOf(string $id): array
    {
        return \array_values(\array_filter(
            $this->titlesWritten(),
            static fn (array $write): bool => $write[0] === $id,
        ));
    }

    /**
     * @return list<array{string, string}>
     */
    private function titlesWrittenAndLocked(): array
    {
        return \array_map(static fn (array $write): array => [$write[0], $write[1]], $this->writes);
    }

    /**
     * @param  Closure(): void  $callback
     */
    private function inCoroutine(Closure $callback): void
    {
        $failure = null;
        $hookFlags = Runtime::getHookFlags();

        try {
            run(static function () use ($callback, &$failure): void {
                try {
                    $callback();
                } catch (Throwable $error) {
                    $failure = $error;
                }
            });
        } finally {
            Runtime::setHookFlags($hookFlags);
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * A destination whose reads yield once and whose writes wait for their delay first, so replications interleave
     * with their caller and with each other. Each write, and each read that locks a document, is recorded when it
     * completes; a write of the title 'broken' fails. A read that locks a document waits for the delay of LOCKED.
     */
    private function yieldingAdapter(): SQLite
    {
        $delay = fn (string $title): float => $this->delays[$title] ?? 0.0;
        $record = function (string $id, string $title): void {
            $coroutine = Coroutine::getCid();
            $this->assertIsInt($coroutine);
            $this->writes[] = [$id, $title, $coroutine];
        };
        $busy = function (int $change): void {
            $this->busy += $change;
            $this->peak = \max($this->peak, $this->busy);
        };

        return new class (new PDO('sqlite::memory:'), $delay, $record, $busy) extends SQLite {
            /**
             * @param  Closure(string): float  $delay
             * @param  Closure(string, string): void  $record
             * @param  Closure(int): void  $busy
             */
            public function __construct(PDO $pdo, private readonly Closure $delay, private readonly Closure $record, private readonly Closure $busy)
            {
                parent::__construct($pdo);
            }

            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                return $this->busy(function () use ($collection, $id, $queries, $forUpdate): Document {
                    if (Coroutine::getCid() > 0) {
                        Coroutine::sleep(0.001);
                    }
                    if ($forUpdate) {
                        $this->wait(MirrorReplicationTest::LOCKED);
                        $this->written($id, MirrorReplicationTest::LOCKED);
                    }

                    return parent::getDocument($collection, $id, $queries, $forUpdate);
                });
            }

            public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value, string $updatedAt, int|float|string|null $min = null, int|float|string|null $max = null): bool
            {
                return $this->busy(function () use ($collection, $id, $attribute, $value, $updatedAt, $min, $max): bool {
                    $increased = parent::increaseDocumentAttribute($collection, $id, $attribute, $value, $updatedAt, $min, $max);
                    $this->written($id, MirrorReplicationTest::INCREASED);

                    return $increased;
                });
            }

            public function createDocuments(Document $collection, array $documents): array
            {
                return $this->busy(function () use ($collection, $documents): array {
                    $this->wait($documents[0]->getAttribute('title', ''));
                    $created = parent::createDocuments($collection, $documents);
                    foreach ($documents as $document) {
                        $this->written($document->getId(), $document->getAttribute('title', ''));
                    }

                    return $created;
                });
            }

            public function createDocument(Document $collection, Document $document): Document
            {
                return $this->busy(function () use ($collection, $document): Document {
                    $this->wait($document->getAttribute('title', ''));
                    $created = parent::createDocument($collection, $document);
                    $this->written($document->getId(), $document->getAttribute('title', ''));

                    return $created;
                });
            }

            public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
            {
                return $this->busy(function () use ($collection, $id, $document, $skipPermissions): Document {
                    $this->wait($document->getAttribute('title', ''));
                    $updated = parent::updateDocument($collection, $id, $document, $skipPermissions);
                    $this->written($id, $document->getAttribute('title', ''));

                    return $updated;
                });
            }

            public function updateDocuments(Document $collection, Document $updates, array $documents): int
            {
                return $this->busy(function () use ($collection, $updates, $documents): int {
                    $this->wait($updates->getAttribute('title', ''));
                    $modified = parent::updateDocuments($collection, $updates, $documents);
                    foreach ($documents as $document) {
                        $this->written($document->getId(), $updates->getAttribute('title', ''));
                    }

                    return $modified;
                });
            }

            /**
             * @param  array<Change>  $changes
             * @return array<Document>
             */
            public function upsertDocuments(Document $collection, string $attribute, array $changes): array
            {
                return $this->busy(function () use ($collection, $attribute, $changes): array {
                    $title = $changes[0]->getNew()->getAttribute('title', '');
                    $this->wait($title);
                    if ($title === 'broken') {
                        throw new RuntimeException('destination rejected broken');
                    }
                    $upserted = parent::upsertDocuments($collection, $attribute, $changes);
                    foreach ($changes as $change) {
                        $this->written($change->getNew()->getId(), $change->getNew()->getAttribute('title', ''));
                    }

                    return $upserted;
                });
            }

            public function deleteDocument(string $collection, string $id): bool
            {
                return $this->busy(function () use ($collection, $id): bool {
                    $this->wait(MirrorReplicationTest::DELETED);
                    $deleted = parent::deleteDocument($collection, $id);
                    $this->written($id, MirrorReplicationTest::DELETED);

                    return $deleted;
                });
            }

            /**
             * @template T
             *
             * @param  Closure(): T  $call
             * @return T
             */
            private function busy(Closure $call): mixed
            {
                ($this->busy)(1);

                try {
                    return $call();
                } finally {
                    ($this->busy)(-1);
                }
            }

            private function wait(mixed $title): void
            {
                $seconds = ($this->delay)(\is_string($title) ? $title : '');
                if ($seconds > 0 && Coroutine::getCid() > 0) {
                    Coroutine::sleep($seconds);
                }
            }

            private function written(string $id, mixed $title): void
            {
                ($this->record)($id, \is_string($title) ? $title : '');
            }
        };
    }
}
