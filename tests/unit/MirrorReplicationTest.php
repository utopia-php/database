<?php

namespace Tests\Unit;

use Closure;
use DateTime;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Swoole\Coroutine;
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
use Utopia\Database\Mirror;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

use function Swoole\Coroutine\run;

final class MirrorReplicationTest extends TestCase
{
    private const string NOTES = 'notes';

    private const string SECRETS = 'secrets';

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

    public function testWritesToDifferentDocumentsDoNotWaitForEachOther(): void
    {
        $this->delays = ['slow' => 0.05, 'fast' => 0.01];

        $this->inCoroutine(function (): void {
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'first', 'title' => 'slow'])]);
            $this->mirror->createDocuments(self::NOTES, [new Document([Document::ID => 'second', 'title' => 'fast'])]);
        });

        $this->assertSame([], $this->errors);
        $this->assertSame([['second', 'fast'], ['first', 'slow']], $this->titlesWritten());
    }

    public function testFinishedReplicationsAreReleased(): void
    {
        $mirror = new class ($this->mirror->getSource(), $this->destination) extends Mirror {
            public function countPendingReplications(): int
            {
                $pending = \count($this->collectionReplications);
                foreach ($this->documentReplications as $documents) {
                    $pending += \count($documents);
                }

                return $pending;
            }
        };
        $this->delays = ['v0' => 0.01, 'v1' => 0.01];
        $pending = null;

        $this->inCoroutine(function () use ($mirror, &$pending): void {
            $mirror->createDocuments(self::NOTES, [
                new Document([Document::ID => 'first', 'title' => 'v0']),
                new Document([Document::ID => 'second', 'title' => 'v0']),
            ]);
            $mirror->updateDocuments(self::NOTES, new Document(['title' => 'v1']));
            $mirror->deleteDocument(self::NOTES, 'public');
            $pending = $mirror->countPendingReplications();
        });

        $this->assertSame(2, $pending, 'The bulk update waits on the create, and the delete on the bulk update');
        $this->assertSame(0, $mirror->countPendingReplications());
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
     * completes; a write of the title 'broken' fails.
     */
    private function yieldingAdapter(): SQLite
    {
        $delay = fn (string $title): float => $this->delays[$title] ?? 0.0;
        $record = function (string $id, string $title): void {
            $this->writes[] = [$id, $title, Coroutine::getCid()];
        };

        return new class (new PDO('sqlite::memory:'), $delay, $record) extends SQLite {
            /**
             * @param  Closure(string): float  $delay
             * @param  Closure(string, string): void  $record
             */
            public function __construct(PDO $pdo, private readonly Closure $delay, private readonly Closure $record)
            {
                parent::__construct($pdo);
            }

            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                if (Coroutine::getCid() > 0) {
                    Coroutine::sleep(0.001);
                }
                if ($forUpdate) {
                    $this->written($id, MirrorReplicationTest::LOCKED);
                }

                return parent::getDocument($collection, $id, $queries, $forUpdate);
            }

            public function increaseDocumentAttribute(string $collection, string $id, string $attribute, int|float|string $value, string $updatedAt, int|float|string|null $min = null, int|float|string|null $max = null): bool
            {
                $increased = parent::increaseDocumentAttribute($collection, $id, $attribute, $value, $updatedAt, $min, $max);
                $this->written($id, MirrorReplicationTest::INCREASED);

                return $increased;
            }

            public function createDocuments(Document $collection, array $documents): array
            {
                $this->wait($documents[0]->getAttribute('title', ''));
                $created = parent::createDocuments($collection, $documents);
                foreach ($documents as $document) {
                    $this->written($document->getId(), $document->getAttribute('title', ''));
                }

                return $created;
            }

            public function createDocument(Document $collection, Document $document): Document
            {
                $this->wait($document->getAttribute('title', ''));
                $created = parent::createDocument($collection, $document);
                $this->written($document->getId(), $document->getAttribute('title', ''));

                return $created;
            }

            public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
            {
                $this->wait($document->getAttribute('title', ''));
                $updated = parent::updateDocument($collection, $id, $document, $skipPermissions);
                $this->written($id, $document->getAttribute('title', ''));

                return $updated;
            }

            public function updateDocuments(Document $collection, Document $updates, array $documents): int
            {
                $this->wait($updates->getAttribute('title', ''));
                $modified = parent::updateDocuments($collection, $updates, $documents);
                foreach ($documents as $document) {
                    $this->written($document->getId(), $updates->getAttribute('title', ''));
                }

                return $modified;
            }

            /**
             * @param  array<Change>  $changes
             * @return array<Document>
             */
            public function upsertDocuments(Document $collection, string $attribute, array $changes): array
            {
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
            }

            public function deleteDocument(string $collection, string $id): bool
            {
                $this->wait(MirrorReplicationTest::DELETED);
                $deleted = parent::deleteDocument($collection, $id);
                $this->written($id, MirrorReplicationTest::DELETED);

                return $deleted;
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
