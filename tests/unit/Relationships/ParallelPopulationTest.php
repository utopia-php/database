<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Runtime;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

use function Swoole\Coroutine\run;

/**
 * Relationship population reads more related ids than one query may carry in chunks, and on a pooled adapter
 * inside a coroutine it reads the chunks at the same time. Every SELECT made while population runs yields for a
 * time that grows with the number of SELECTs already in flight, so the chunks start and finish in the same order
 * on every run.
 */
final class ParallelPopulationTest extends TestCase
{
    private const int ROUNDS = 3;

    private const int DOCUMENTS = 6;

    private string $file;

    private bool $latency = false;

    private int $inFlight = 0;

    private int $peak = 0;

    private int $finds = 0;

    private bool $selectedInACoroutine = false;

    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutine population');
        }

        $file = \tempnam(\sys_get_temp_dir(), 'parallel-population-');
        $this->assertIsString($file);
        $this->file = $file;
    }

    protected function tearDown(): void
    {
        if (isset($this->file) && \is_file($this->file)) {
            \unlink($this->file);
        }
    }

    public function testParallelPopulationLeavesAuthorizationAndRelationshipsEnabled(): void
    {
        $this->inCoroutine(function (): void {
            $database = $this->database($this->pool());

            for ($round = 1; $round <= self::ROUNDS; $round++) {
                $this->assertCount(self::DOCUMENTS, $this->findParents($database));
                $this->assertTrue($database->getAuthorization()->getStatus(), "Authorization is left disabled after round {$round}");
                $this->assertTrue($this->hook($database)->isEnabled(), "Relationships are left disabled after round {$round}");
            }
        });
    }

    public function testParallelPopulationReturnsNoRelatedDocumentWithoutAReadGrant(): void
    {
        $this->inCoroutine(function (): void {
            $database = $this->database($this->pool());

            for ($round = 1; $round <= self::ROUNDS; $round++) {
                $parents = $this->findParents($database);

                $this->assertSame([], $this->populatedSecrets($parents), "Round {$round} populated secrets the caller cannot read");
                $this->assertSame([], $this->ids($database->find('secrets')), "Round {$round} left secrets readable");
            }
        });
    }

    public function testSilentCoversTheParallelReads(): void
    {
        $this->inCoroutine(function (): void {
            $database = $this->database($this->pool());

            $this->finds = 0;
            $this->findParents($database);
            $this->assertSame(1, $this->finds, 'Population delivered find events of its own');

            $this->finds = 0;
            $database->silent(fn (): array => $this->findParents($database));
            $this->assertSame(0, $this->finds, 'Find events were delivered inside silent()');
        });
    }

    public function testPopulationRunsItsChunksInParallel(): void
    {
        $this->inCoroutine(function (): void {
            $database = $this->database($this->pool());

            $this->findParents($database);

            $this->assertGreaterThan(1, $this->peak, 'Population read its chunks one at a time');
        });
    }

    public function testPopulationOnAnAdapterWithoutAPoolReadsOneChunkAtATime(): void
    {
        $this->inCoroutine(function (): void {
            $database = $this->database($this->sqlite());

            for ($round = 1; $round <= self::ROUNDS; $round++) {
                $parents = $this->findParents($database);

                $this->assertSame([], $this->populatedSecrets($parents));
                $this->assertTrue($database->getAuthorization()->getStatus());
                $this->assertTrue($this->hook($database)->isEnabled());
            }

            $this->assertSame(1, $this->peak, 'Chunk reads shared one connection at the same time');
        });
    }

    public function testPopulationInsideATransactionReadsOneChunkAtATime(): void
    {
        $this->inCoroutine(function (): void {
            $database = $this->database($this->pool());

            $parents = $database->withTransaction(fn (): array => $this->findParents($database));

            $this->assertCount(self::DOCUMENTS, $parents);
            $this->assertSame(\array_map(fn (int $index): string => "label{$index}", \range(1, self::DOCUMENTS)), \array_map(
                fn (Document $parent): string => $parent->getDocuments('labels')[0]->getId(),
                $parents,
            ));
            $this->assertSame(1, $this->peak, 'Chunk reads ran at the same time inside a transaction');
            $this->assertTrue($database->getAuthorization()->getStatus());
        });
    }

    public function testPopulationOutsideACoroutineKeepsTheCallersState(): void
    {
        $database = $this->database($this->pool());
        $this->selectedInACoroutine = false;

        $parents = $database->find('parents', [Query::limit(self::DOCUMENTS)]);

        $this->assertFalse($this->selectedInACoroutine, 'Population started coroutines outside a scheduler');

        $this->assertCount(self::DOCUMENTS, $parents);
        $this->assertSame([], $this->populatedSecrets($parents));
        $this->assertTrue($database->getAuthorization()->getStatus());
        $this->assertTrue($this->hook($database)->isEnabled());
        $this->assertSame([], $this->ids($database->find('secrets')));

        $this->finds = 0;
        $database->silent(fn (): array => $database->find('parents', [Query::limit(self::DOCUMENTS)]));
        $this->assertSame(0, $this->finds, 'Find events were delivered inside silent()');
    }

    private function inCoroutine(Closure $test): void
    {
        $hookFlags = Runtime::getHookFlags();
        $failure = null;

        try {
            run(static function () use ($test, &$failure): void {
                try {
                    $test();
                } catch (\Throwable $error) {
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
     * @return array<Document>
     */
    private function findParents(Database $database): array
    {
        $this->latency = true;

        try {
            return $database->find('parents', [Query::limit(self::DOCUMENTS)]);
        } finally {
            $this->latency = false;
        }
    }

    private function pool(): Pool
    {
        return new Pool(new UtopiaPool(new Stack(), 'parallel-population', 8, $this->sqlite(...), timeout: 1.0));
    }

    private function sqlite(): SQLite
    {
        return new SQLite(new PDO('sqlite:' . $this->file));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization(new Authorization());
        $database->setDatabase('population')->setNamespace('population');
        $database->create();
        $database->addHook(new Permissions());
        $database->addHook(new Relationships($database));
        $database->addHook(new class ($this->select(...)) implements Transform {
            public function __construct(private readonly Closure $select)
            {
            }

            public function transform(Event $event, string $query): string
            {
                if (\stripos($query, 'select') !== false) {
                    ($this->select)();
                }

                return $query;
            }
        });
        $database->addHook(new class ($this->record(...)) implements Lifecycle {
            public function __construct(private readonly Closure $record)
            {
            }

            public function handle(Event $event, mixed $data): void
            {
                ($this->record)($event);
            }
        });

        $open = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
        $database->createCollection(new Collection(id: 'parents', permissions: $open, documentSecurity: false, attributes: [Attribute::string('name', 32)]));
        $database->createCollection(new Collection(id: 'children', permissions: $open, documentSecurity: false, attributes: [Attribute::string('name', 32)]));
        $database->createCollection(new Collection(id: 'labels', permissions: $open, documentSecurity: false, attributes: [Attribute::string('name', 32)]));
        $database->createCollection(new Collection(id: 'secrets', permissions: [Permission::create(Role::any())], documentSecurity: true, attributes: [Attribute::string('name', 32)]));
        $database->createRelationship(new Relationship(collection: 'parents', relatedCollection: 'children', type: RelationType::OneToMany, twoWay: true, key: 'children', twoWayKey: 'parent'));
        $database->createRelationship(new Relationship(collection: 'parents', relatedCollection: 'labels', type: RelationType::ManyToMany, twoWay: true, key: 'labels', twoWayKey: 'parents'));
        $database->createRelationship(new Relationship(collection: 'parents', relatedCollection: 'secrets', type: RelationType::ManyToOne, twoWay: false, key: 'secret', twoWayKey: 'parents'));

        $database->getAuthorization()->skip(function () use ($database, $open): void {
            for ($index = 1; $index <= self::DOCUMENTS; $index++) {
                $database->createDocument('secrets', new Document(['$id' => "secret{$index}", 'name' => "secret {$index}", '$permissions' => [Permission::read(Role::user('owner'))]]));
                $database->createDocument('labels', new Document(['$id' => "label{$index}", 'name' => "label {$index}", '$permissions' => $open]));
                $database->createDocument('parents', new Document([
                    '$id' => "parent{$index}",
                    'name' => "parent {$index}",
                    '$permissions' => $open,
                    'children' => [new Document(['$id' => "child{$index}", 'name' => "child {$index}", '$permissions' => $open])],
                    'labels' => ["label{$index}"],
                    'secret' => "secret{$index}",
                ]));
            }
        });

        $database->setMaxQueryValues(2);
        $this->peak = 0;

        return $database;
    }

    private function select(): void
    {
        if (Coroutine::getCid() > 0) {
            $this->selectedInACoroutine = true;
        }

        if (! $this->latency || Coroutine::getCid() <= 0) {
            return;
        }

        $this->inFlight++;
        $this->peak = \max($this->peak, $this->inFlight);

        try {
            Coroutine::sleep(0.002 * $this->inFlight);
        } finally {
            $this->inFlight--;
        }
    }

    private function record(Event $event): void
    {
        if ($event === Event::DocumentFind) {
            $this->finds++;
        }
    }

    private function hook(Database $database): Relationships
    {
        $hook = $database->getRelationshipHook();
        $this->assertNotNull($hook);

        return $hook;
    }

    /**
     * @param  array<Document>  $parents
     * @return array<string>
     */
    private function populatedSecrets(array $parents): array
    {
        $secrets = [];
        foreach ($parents as $parent) {
            $secret = $parent->getAttribute('secret');
            if ($secret instanceof Document && ! $secret->isEmpty()) {
                $secrets[] = $secret->getId();
            }
        }

        return $secrets;
    }

    /**
     * @param  array<Document>  $documents
     * @return array<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(static fn (Document $document): string => $document->getId(), $documents);
    }
}
