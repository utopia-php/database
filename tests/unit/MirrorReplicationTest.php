<?php

namespace Tests\Unit;

use Closure;
use PDO;
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
use Utopia\Database\Validator\Authorization;

use function Swoole\Coroutine\run;

final class MirrorReplicationTest extends TestCase
{
    private const string NOTES = 'notes';

    private const string SECRETS = 'secrets';

    private const string DELETED = 'deleted';

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
                attributes: [Attribute::string(key: 'title', size: 64)],
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

        $this->assertSame([['first', 'v0', -1], ['public', self::DELETED, -1]], $this->writes);
        $this->assertSame('v0', $this->destination->getDocument(self::NOTES, 'first')->getAttribute('title'));
        $this->assertTrue($this->destination->getDocument(self::NOTES, 'public')->isEmpty());
    }

    /**
     * @return list<array{string, string}>
     */
    private function titlesWritten(): array
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
     * with their caller and with each other. Each write is recorded when it completes; a write of the title 'broken'
     * fails.
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

                return parent::getDocument($collection, $id, $queries, $forUpdate);
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
                $this->wait('deleted');
                $deleted = parent::deleteDocument($collection, $id);
                $this->written($id, 'deleted');

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
