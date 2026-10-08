<?php

namespace Tests\Unit\Adapter;

use ArrayObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolCapabilityTest extends TestCase
{
    private int $checkouts = 0;

    private bool $down = false;

    public function testAWarmCachedReadChecksOutNoConnection(): void
    {
        $connections = $this->connections(new Memory());
        $database = $this->database($connections);
        $database->setValidation(false);
        $database->getDocument('posts', 'first');

        $this->checkouts = 0;
        $document = $database->getDocument('posts', 'first');

        $this->assertSame('first', $document->getAttribute('title'));
        $this->assertSame(0, $this->checkouts);
    }

    public function testACachedReadSucceedsWhileTheBackingIsDown(): void
    {
        $connections = $this->connections(new Memory());
        $database = $this->database($connections);
        $database->setValidation(false);
        $database->getDocument('posts', 'first');

        $this->down = true;

        $this->assertSame('first', $database->getDocument('posts', 'first')->getAttribute('title'));
    }

    public function testAWarmValidatedReadOverSchemaEnforcingConnectionsChecksOutNoConnection(): void
    {
        /** @var ArrayObject<int, string> $asked */
        $asked = new ArrayObject();
        $memory = $this->askedMemory($asked);
        $database = $this->database($this->connections($memory));
        $selection = [Query::select(['title'])];
        $database->getDocument('posts', 'first', $selection);

        $this->checkouts = 0;
        $asked->exchangeArray([]);
        $document = $database->getDocument('posts', 'first', $selection);

        $this->assertSame('first', $document->getAttribute('title'));
        $this->assertSame([], $asked->getArrayCopy(), 'A connection without a schemaless mode always answers alike, so its answer is kept');
        $this->assertSame(0, $this->checkouts);
    }

    public function testDefinedAttributesOfSchemaEnforcingConnectionsIsAskedOnce(): void
    {
        $pool = $this->pool($this->connections(new Memory()));
        $this->assertTrue($pool->supports(Capability::DefinedAttributes));

        $this->checkouts = 0;
        $this->down = true;

        $this->assertTrue($pool->supports(Capability::DefinedAttributes));
        $this->assertSame(0, $this->checkouts);
    }

    public function testAWarmValidatedReadWithoutQueriesChecksOutNoConnection(): void
    {
        /** @var ArrayObject<int, string> $asked */
        $asked = new ArrayObject();
        $memory = $this->askedMemory($asked);
        $database = $this->database($this->connections($memory));
        $database->getDocument('posts', 'first');

        $this->checkouts = 0;
        $asked->exchangeArray([]);
        $database->getDocument('posts', 'first');

        $this->assertSame([], $asked->getArrayCopy());
        $this->assertSame(0, $this->checkouts);
    }

    /**
     * @param  ArrayObject<int, string>  $asked
     */
    private function askedMemory(ArrayObject $asked): Memory
    {
        return new class ($asked) extends Memory {
            /**
             * @param  ArrayObject<int, string>  $asked
             */
            public function __construct(private readonly ArrayObject $asked)
            {
                parent::__construct();
            }

            #[\Override]
            public function supports(Capability $feature): bool
            {
                $this->asked->append($feature->name);

                return parent::supports($feature);
            }
        };
    }

    public function testACapabilityQuestionOnAColdPoolChecksOutOnce(): void
    {
        $pool = $this->pool($this->connections(new Memory()));

        $this->assertTrue($pool->supports(Capability::Operators));
        $this->assertFalse($pool->supports(Capability::AlterLock));
        $this->assertSame((new Memory())->capabilities(), $pool->capabilities());
        $this->assertSame(1, $this->checkouts);
    }

    public function testEveryHandleOverOnePoolSharesTheAnswers(): void
    {
        $connections = $this->connections(new Memory());
        $this->assertTrue($this->pool($connections)->supports(Capability::Operators));

        $this->checkouts = 0;
        $this->down = true;

        $handle = $this->pool($connections);
        $this->assertTrue($handle->supports(Capability::Operators));
        $this->assertTrue($handle->supports(Capability::IndexFulltext));
        $this->assertSame(0, $this->checkouts);
    }

    public function testFeaturesAreAskedOncePerFeature(): void
    {
        $pool = $this->pool($this->connections(new Memory()));

        $this->assertTrue($pool->hasFeature(Feature\Relationships::class));
        $this->assertFalse($pool->hasFeature(Feature\Spatial::class));
        $this->assertSame(2, $this->checkouts);

        $this->down = true;

        $this->assertTrue($pool->hasFeature(Feature\Relationships::class));
        $this->assertFalse($pool->hasFeature(Feature\Spatial::class));
        $this->assertSame(2, $this->checkouts);
    }

    public function testDefinedAttributesAlwaysAsksTheConnection(): void
    {
        $mongo = new class () extends Mongo {
            public function __construct()
            {
            }
        };
        $pool = $this->pool($this->connections($mongo));
        $this->assertTrue($pool->hasFeature(Feature\Schemaless::class));
        $this->checkouts = 0;

        $mongo->setSchemaless(true);
        $this->assertFalse($pool->supports(Capability::DefinedAttributes));

        $mongo->setSchemaless(false);
        $this->assertTrue($pool->supports(Capability::DefinedAttributes));

        $this->assertSame(2, $this->checkouts);
        $this->assertTrue($mongo->supports(Capability::DefinedAttributes), "A handle that never set the schema mode must leave the connection's own");
    }

    public function testDefinedAttributesIsAskedOncePerModeTheHandleSet(): void
    {
        $mongo = new class () extends Mongo {
            public function __construct()
            {
            }
        };
        $connections = $this->connections($mongo);
        $pool = $this->pool($connections);

        $pool->setSchemaless(true);
        $this->checkouts = 0;
        $this->assertFalse($pool->supports(Capability::DefinedAttributes));
        $this->assertFalse($pool->supports(Capability::DefinedAttributes));
        $this->assertSame(1, $this->checkouts, 'A schema mode the handle set is asked of a connection once');

        $mongo->setSchemaless(false);
        $this->assertFalse($pool->supports(Capability::DefinedAttributes), 'Every connection the handle borrows is put in its mode first');

        $pool->setSchemaless(false);
        $this->checkouts = 0;
        $this->assertTrue($pool->supports(Capability::DefinedAttributes));
        $this->assertTrue($pool->supports(Capability::DefinedAttributes));
        $this->assertSame(1, $this->checkouts, 'Each mode is answered by a connection in that mode');

        $other = $this->pool($connections);
        $other->setSchemaless(true);
        $this->checkouts = 0;
        $this->assertFalse($other->supports(Capability::DefinedAttributes));
        $this->assertTrue($pool->supports(Capability::DefinedAttributes));
        $this->assertSame(0, $this->checkouts, 'Handles over one pool share the answer of each mode');
    }

    public function testTheProfileFollowsTheConnectionsSchemaModeWhileThePoolLeavesItUnset(): void
    {
        $mongo = new class () extends Mongo {
            public function __construct()
            {
            }
        };
        $database = new Database($this->pool($this->connections($mongo)), new Cache(new MemoryCache()));
        $profile = $database->profile();

        $mongo->setSchemaless(true);
        $this->assertFalse($profile->supports(Capability::DefinedAttributes));
        $this->assertFalse($database->profile()->supports(Capability::DefinedAttributes));

        $mongo->setSchemaless(false);
        $this->assertTrue($profile->supports(Capability::DefinedAttributes));
        $this->assertTrue($database->profile()->supports(Capability::DefinedAttributes));
    }

    public function testSettingTheSchemaModeOnAPoolOfSchemaEnforcingAdaptersLeavesThemEnforcingIt(): void
    {
        $connections = $this->connections(new Memory());
        $pool = $this->pool($connections);
        $this->assertFalse($pool->hasFeature(Feature\Schemaless::class));
        $this->down = true;

        $this->assertSame($pool, $pool->setSchemaless(true), 'Setting the mode must not need a connection');
        $this->assertSame($pool, $pool->setSchemaless(false));

        $this->down = false;
        $pool->setSchemaless(true);
        $this->assertTrue($pool->supports(Capability::DefinedAttributes));

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support schemaless');
        $pool->isSchemaless();
    }

    public function testAFailedFirstCheckoutAnswersNothingAndTheNextOneFillsTheAnswers(): void
    {
        $pool = $this->pool($this->connections(new Memory()));
        $this->down = true;

        $caught = null;
        try {
            $pool->supports(Capability::Operators);
        } catch (RuntimeException $exception) {
            $caught = $exception;
        }
        $this->assertSame('backing unreachable', $caught?->getMessage(), 'A capability question with no answer yet must fail while the backing is down');

        $this->down = false;
        $this->assertTrue($pool->supports(Capability::Operators));

        $this->down = true;
        $this->assertTrue($pool->supports(Capability::Operators));
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function connections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            function (callable $callback) use ($adapter): mixed {
                if ($this->down) {
                    throw new RuntimeException('backing unreachable');
                }

                $this->checkouts++;

                return $callback($adapter);
            },
        );

        return $connections;
    }

    /**
     * @param  UtopiaPool<Adapter>  $connections
     */
    private function pool(UtopiaPool $connections): Pool
    {
        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }

    /**
     * @param  UtopiaPool<Adapter>  $connections
     */
    private function database(UtopiaPool $connections): Database
    {
        $database = new Database($this->pool($connections), new Cache(new MemoryCache()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('pool_capabilities')
            ->setNamespace('pool_capabilities_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'posts',
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument('posts', new Document([
            Document::ID => 'first',
            'title' => 'first',
            Document::PERMISSIONS => [Permission::read(Role::any())],
        ]));

        return $database;
    }
}
