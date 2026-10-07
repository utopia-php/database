<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Transaction as TransactionException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Operator;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;
use Utopia\Query\Method;

#[RequiresPhpExtension('redis')]
final class RedisAdapterPathsTest extends TestCase
{
    private const string NAMESPACE = 'redis_paths';

    private const string DATABASE = 'redis_paths';

    private const string NOTES = 'notes';

    private const int TENANT = 5;

    private Authorization $authorization;

    private Redis $client;

    /** @var array<string, string> */
    private array $strings = [];

    /** @var array<string, array<string, true>> */
    private array $sets = [];

    /** @var array<string, array<string, string>> */
    private array $hashes = [];

    private bool $pipelining = false;

    /** @var list<mixed> */
    private array $queued = [];

    /** @var list<string> */
    private array $hashWrites = [];

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
        $this->client = $this->fakeClient();
    }

    public function testOneToManyKeyRenamedFromTheChildSide(): void
    {
        $database = $this->petsDatabase(RelationshipType::OneToMany, key: 'pets', twoWayKey: 'owner');
        $database->createDocument('owners', new Document(['$id' => 'alice', 'name' => 'Alice', 'pets' => [
            new Document(['$id' => 'rex', 'name' => 'Rex']),
        ]]));

        $database->updateRelationship('pets', 'owner', new RelationshipUpdate(key: 'keeper'));

        $pet = $database->getDocument('pets', 'rex');
        $this->assertNull($pet->getAttribute('owner'));
        $this->assertSame('alice', $this->idOf($pet->getAttribute('keeper')));
        $this->assertSame(['rex'], $this->idsOf($database->getDocument('owners', 'alice')->getAttribute('pets')));
    }

    public function testManyToOneTwoWayKeyRenamedFromTheChildSide(): void
    {
        $database = $this->petsDatabase(RelationshipType::ManyToOne, key: 'owner', twoWayKey: 'pets', from: 'pets', to: 'owners');
        $database->createDocument('owners', new Document(['$id' => 'alice', 'name' => 'Alice']));
        $database->createDocument('pets', new Document(['$id' => 'rex', 'name' => 'Rex', 'owner' => 'alice']));

        $database->updateRelationship('owners', 'pets', new RelationshipUpdate(twoWayKey: 'master'));

        $pet = $database->getDocument('pets', 'rex');
        $this->assertNull($pet->getAttribute('owner'));
        $this->assertSame('alice', $this->idOf($pet->getAttribute('master')));
        $this->assertSame(['rex'], $this->idsOf($database->getDocument('owners', 'alice')->getAttribute('pets')));
    }

    public function testUniqueIndexOverExistingDuplicatesIsRefusedByTheAdapter(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        foreach (['first', 'second'] as $id) {
            $adapter->createDocument($this->notes(), new Document(['$id' => $id, '$permissions' => [], 'title' => 'same']));
        }

        try {
            $adapter->createIndex(self::NOTES, Index::unique(key: 'unique_title', attributes: ['title']));
            $this->fail('A unique index over duplicate values must be refused');
        } catch (DuplicateException $exception) {
            $this->assertSame('Cannot create unique index: existing rows already contain duplicate values', $exception->getMessage());
        }

        $this->assertTrue($adapter->createIndex(self::NOTES, Index::key(key: 'by_title', attributes: ['title'])));
        try {
            $adapter->createIndex(self::NOTES, Index::key(key: 'by_title', attributes: ['title']));
            $this->fail('An index id that is already recorded must be refused');
        } catch (DuplicateException $exception) {
            $this->assertSame('Index already exists', $exception->getMessage());
        }

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $adapter->createIndex('missing', Index::key(key: 'by_title', attributes: ['title']));
    }

    public function testNoRolesSeeNoDocumentsInADocumentSecurityCollection(): void
    {
        $database = $this->database();
        $database->create();
        $database->createCollection(Collection::create(
            id: self::NOTES,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));

        $this->assertSame([], $database->find(self::NOTES), 'An empty collection has no ids to filter');

        $database->createDocument(self::NOTES, new Document(['$id' => 'public', 'title' => 'a', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument(self::NOTES, new Document(['$id' => 'private', 'title' => 'b', '$permissions' => [Permission::read(Role::user('bob'))]]));

        $this->assertSame(['public'], $this->idsOf($database->find(self::NOTES)));
        $this->assertSame(['private', 'public'], $this->sorted($this->authorization->skip(fn (): array => $this->idsOf($database->find(self::NOTES)))));

        $this->authorization->cleanRoles();
        try {
            $this->assertSame([], $database->find(self::NOTES), 'A caller with no roles must see no documents');
            $this->assertSame(0, $database->count(self::NOTES));
        } finally {
            $this->authorization->addRole(Role::any()->toString());
        }
    }

    public function testRollbackRestoresARenamedDocument(): void
    {
        $database = $this->database();
        $database->create();
        $database->createCollection(Collection::create(
            id: self::NOTES,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::NOTES, new Document(['$id' => 'a', 'title' => 'original']));

        $rethrown = false;
        try {
            $database->withTransaction(function () use ($database): void {
                $database->updateDocument(self::NOTES, 'a', new Document(['$id' => 'b', 'title' => 'renamed']));
                throw new \RuntimeException('roll back');
            });
        } catch (\RuntimeException $exception) {
            $rethrown = $exception->getMessage() === 'roll back';
        }
        $this->assertTrue($rethrown, 'The transaction must rethrow');

        $this->assertSame('original', $database->getDocument(self::NOTES, 'a')->getAttribute('title'));
        $this->assertTrue($database->getDocument(self::NOTES, 'b')->isEmpty());
        $this->assertSame([], $database->find(self::NOTES, [Query::equal('$id', ['b'])]));
        $this->assertSame(['a'], $this->idsOf($database->find(self::NOTES)));
    }

    public function testRollbackRefusesAnUnknownJournalEntry(): void
    {
        $adapter = new class ($this->client) extends RedisAdapter {
            public function journalUnknownEntry(): void
            {
                $this->journal('unknown', []);
            }
        };

        $adapter->startTransaction();
        $adapter->journalUnknownEntry();

        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('Unknown journal op: unknown');
        $adapter->rollbackTransaction();
    }

    public function testRenamingAManyToManyKeyOfATenantlessDefinition(): void
    {
        $database = $this->database()->setSharedTables(true)->setTenant(null);
        $database->create();
        foreach (['books', 'authors'] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
        }
        $database->createRelationship('books', Relationship::manyToMany(
            relatedCollection: 'authors',
            twoWay: true,
            key: 'authors',
            twoWayKey: 'books',
        ));
        $junction = '_'.$database->getCollection('books')->getSequence().'_'.$database->getCollection('authors')->getSequence();

        $adapter = $database->getAdapter();
        $this->assertInstanceOf(RedisAdapter::class, $adapter);
        $adapter->setTenant(self::TENANT);
        $adapter->createDocument(new Document(['$id' => $junction]), new Document([
            '$id' => 'link',
            '$permissions' => [],
            '$tenant' => self::TENANT,
            'authors' => 'ann',
            'books' => 'dune',
        ]));

        $this->assertTrue($adapter->updateRelationship(
            'books',
            Relationship::manyToMany(relatedCollection: 'authors', twoWay: true, key: 'authors', twoWayKey: 'books'),
            RelationshipSide::Parent,
            new RelationshipUpdate(key: 'writers'),
        ));

        $link = $adapter->getDocument(new Document(['$id' => $junction]), 'link');
        $this->assertSame('ann', $link->getAttribute('writers'), 'The junction of a tenantless definition must be found from a tenant');
        $this->assertNull($link->getAttribute('authors'));
    }

    public function testNullChecksAndUnsupportedMethodsOnAWholeObjectAttribute(): void
    {
        $database = $this->database();
        $database->create();
        $database->createCollection(Collection::create(
            id: self::NOTES,
            attributes: [Attribute::object(key: 'meta')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument(self::NOTES, new Document(['$id' => 'filled', 'meta' => ['colour' => 'red']]));
        $database->createDocument(self::NOTES, new Document(['$id' => 'empty', 'meta' => null]));

        $this->assertSame(['empty'], $this->idsOf($database->find(self::NOTES, [Query::isNull('meta')])));
        $this->assertSame(['filled'], $this->idsOf($database->find(self::NOTES, [Query::isNotNull('meta')])));

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Query method lessThan not supported for object attributes');
        $database->skipValidation(fn (): array => $database->find(self::NOTES, [Query::lessThan('meta', 'x')]));
    }

    public function testSchemaChangesOnACollectionWithoutStorage(): void
    {
        $adapter = $this->adapter();

        $this->assertTrue($adapter->deleteIndex('missing', 'by_title'));
        $this->assertTrue($adapter->createRelationship('missing', Relationship::oneToOne(relatedCollection: 'gone', twoWay: true, key: 'partner', twoWayKey: 'partnerOf')));
        $this->assertSame(0, $adapter->getSizeOfCollection('missing'));
        $this->assertSame([], $this->hashWrites, 'A collection without storage must not be written to');

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');
        $adapter->renameIndex('missing', 'by_title', 'by_name');
    }

    public function testRenamingAnIndexTheCollectionDoesNotRecordWritesNothingAndReportsNothingRenamed(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        $adapter->createIndex(self::NOTES, Index::key(key: 'by_title', attributes: ['title']));
        $this->hashWrites = [];

        $this->assertFalse($adapter->renameIndex(self::NOTES, 'absent', 'other'));
        $this->assertSame([], $this->hashWrites);

        $this->assertTrue($adapter->renameIndex(self::NOTES, 'by_title', 'by_name'));
        $this->assertCount(1, $this->recordedHashWrites());
    }

    public function testRenamingAnIndexTheCollectionAlreadyRenamedReportsItRenamed(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        $adapter->createIndex(self::NOTES, Index::key(key: 'by_name', attributes: ['title']));
        $this->hashWrites = [];

        $this->assertTrue($adapter->renameIndex(self::NOTES, 'by_title', 'by_name'));
        $this->assertSame([], $this->hashWrites);
    }

    public function testGetSequencesBackFillsOnlyTheDocumentsThatLackOne(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        $stored = $adapter->createDocument($this->notes(), new Document(['$id' => 'stored', '$permissions' => [], 'title' => 'a']));

        $this->assertSame([], $adapter->getSequences(new Document(['$id' => self::NOTES]), []));

        $documents = $adapter->getSequences(new Document(['$id' => self::NOTES]), [
            new Document(['$id' => 'stored']),
            new Document(['$id' => 'missing']),
            new Document(['$id' => 'given', '$sequence' => '99']),
        ]);

        $this->assertSame($stored->getSequence(), $documents[0]->getSequence());
        $this->assertEmpty($documents[1]->getSequence());
        $this->assertSame('99', $documents[2]->getSequence());

        $complete = $adapter->getSequences(new Document(['$id' => self::NOTES]), [new Document(['$id' => 'given', '$sequence' => '99'])]);
        $this->assertSame('99', $complete[0]->getSequence());
    }

    public function testGetSequencesReportsAFailingPipeline(): void
    {
        $client = self::createStub(Redis::class);
        $client->method('multi')->willReturnSelf();
        $client->method('get')->willReturnSelf();
        $client->method('exec')->willThrowException(new \RedisException('connection lost'));

        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('Failed to load sequences: connection lost');
        (new RedisAdapter($client))->getSequences(new Document(['$id' => self::NOTES]), [new Document(['$id' => 'first'])]);
    }

    public function testIncrementGuardsOfTheAdapter(): void
    {
        $adapter = $this->adapter();
        $adapter->createCollection(self::NOTES, [Attribute::double(key: 'count')]);
        $adapter->createDocument($this->notes(), new Document(['$id' => 'whole', '$permissions' => [], 'count' => 10]));
        $adapter->createDocument($this->notes(), new Document(['$id' => 'fraction', '$permissions' => [], 'count' => 10.5]));

        foreach (['whole' => 10, 'fraction' => 10.5] as $id => $stored) {
            $this->assertTrue($adapter->increaseDocumentAttribute(new Document(['$id' => self::NOTES]), $id, 'count', 1, '2026-01-01 00:00:00.000', max: 5));
            $this->assertTrue($adapter->increaseDocumentAttribute(new Document(['$id' => self::NOTES]), $id, 'count', -1, '2026-01-01 00:00:00.000', min: 20));
            $this->assertSame($stored, $adapter->getDocument($this->notes(), $id)->getAttribute('count'), $id);
        }

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Document not found');
        $adapter->increaseDocumentAttribute(new Document(['$id' => self::NOTES]), 'vanished', 'count', 1, '2026-01-01 00:00:00.000');
    }

    public function testRenamingAnAttributeOnAnEmptyCollectionOrToItsOwnName(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);

        $this->assertTrue($adapter->renameAttribute(self::NOTES, 'title', 'heading'));
        $adapter->createDocument($this->notes(), new Document(['$id' => 'first', '$permissions' => [], 'heading' => 'kept']));

        $this->assertTrue($adapter->renameAttribute(self::NOTES, 'heading', 'heading'));
        $this->assertSame('kept', $adapter->getDocument($this->notes(), 'first')->getAttribute('heading'));
    }

    public function testUnvalidatedNullCandidateAndUnsupportedMethod(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        foreach (['first' => 'x', 'second' => 'y'] as $id => $title) {
            $adapter->createDocument($this->notes(), new Document(['$id' => $id, '$permissions' => [], 'title' => $title]));
        }

        $this->assertSame([], $adapter->find($this->notes(), [new Query(Method::NotEqual, 'title', [null, 'x'])]), 'A null candidate makes NOT IN unknown for every row');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Query method not supported by Redis adapter: exists');
        $adapter->find($this->notes(), [Query::exists(['title'])]);
    }

    public function testUniqueIndexComparesArrayValuesByContent(): void
    {
        $adapter = $this->adapter();
        $adapter->createCollection(self::NOTES, [Attribute::string(key: 'tags', size: 16, array: true)]);
        $adapter->createIndex(self::NOTES, Index::unique(key: 'unique_tags', attributes: ['tags']));
        $adapter->createDocument($this->notes(), new Document(['$id' => 'first', '$permissions' => [], 'tags' => ['a', 'b']]));
        $adapter->createDocument($this->notes(), new Document(['$id' => 'other', '$permissions' => [], 'tags' => ['b', 'a']]));

        $this->expectException(DuplicateException::class);
        $adapter->createDocument($this->notes(), new Document(['$id' => 'second', '$permissions' => [], 'tags' => ['a', 'b']]));
    }

    public function testFractionalOperatorLimitIsRefusedBeforeTheWrite(): void
    {
        $database = $this->database();
        $database->create();
        $database->createCollection(Collection::create(
            id: self::NOTES,
            attributes: [Attribute::integer(key: 'count'), Attribute::bigInteger(key: 'big')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::NOTES, new Document(['$id' => 'counter', 'count' => 100, 'big' => PHP_INT_MAX - 5]));

        try {
            $database->updateDocument(self::NOTES, 'counter', new Document(['count' => Operator::increment(5, 102.4)]));
            $this->fail('A fractional limit on an integer attribute must be refused');
        } catch (StructureException $exception) {
            $this->assertSame("Invalid document structure: Cannot apply increment operator: max/min limit must be a whole number for integer attribute 'count', got 102.4", $exception->getMessage());
        }
        $this->assertSame(100, $database->getDocument(self::NOTES, 'counter')->getAttribute('count'));

        $database->updateDocument(self::NOTES, 'counter', new Document(['big' => Operator::increment(10, 9.0e18)]));
        $this->assertSame(PHP_INT_MAX - 5, $database->getDocument(self::NOTES, 'counter')->getAttribute('big'));
    }

    public function testACursorWithoutAnOrderPagesBySequence(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        foreach (['first', 'second', 'third'] as $id) {
            $adapter->createDocument($this->notes(), new Document(['$id' => $id, '$permissions' => [], 'title' => $id]));
        }
        $cursor = ['$sequence' => $adapter->getDocument($this->notes(), 'second')->getSequence()];

        $this->assertSame(['third'], $this->idsOf($adapter->find($this->notes(), cursor: $cursor)));
        $this->assertSame(['first'], $this->idsOf($adapter->find($this->notes(), cursor: $cursor, cursorDirection: CursorDirection::Before)));
    }

    public function testRenamingAManyToManyKeyBetweenCollectionsWithoutDefinitionsRenamesNothing(): void
    {
        $adapter = $this->adapter();
        $this->createNotes($adapter);
        $adapter->createCollection('tags', [Attribute::string(key: 'name', size: 64)]);
        $before = [$this->strings, $this->sets, $this->hashes];

        $this->assertTrue($adapter->updateRelationship(
            self::NOTES,
            Relationship::manyToMany(relatedCollection: 'tags', twoWay: true, key: 'tags', twoWayKey: 'notes'),
            RelationshipSide::Parent,
            new RelationshipUpdate(key: 'labels', twoWayKey: 'entries'),
        ));

        $this->assertSame($before, [$this->strings, $this->sets, $this->hashes], 'without stored definitions there is no junction to rename');
    }

    private function petsDatabase(RelationshipType $type, string $key, string $twoWayKey, string $from = 'owners', string $to = 'pets'): Database
    {
        $database = $this->database();
        $database->create();
        foreach (['owners', 'pets'] as $collection) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
            ));
        }
        $database->createRelationship($from, Relationship::fromArray([
            'relatedCollection' => $to,
            'relationType' => $type,
            'twoWay' => true,
            'key' => $key,
            'twoWayKey' => $twoWayKey,
        ]));

        return $database;
    }

    private function database(): Database
    {
        $database = (new Database(new RedisAdapter($this->client), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase(self::DATABASE)
            ->setNamespace(self::NAMESPACE);
        $database->addHook(new Relationships($database));

        return $database;
    }

    private function adapter(): RedisAdapter
    {
        $authorization = new Authorization();
        $authorization->disable();
        $adapter = new RedisAdapter($this->client);
        $adapter->setAuthorization($authorization);
        $adapter->setDatabase(self::DATABASE);
        $adapter->setNamespace(self::NAMESPACE);
        $adapter->create(self::DATABASE);

        return $adapter;
    }

    private function createNotes(RedisAdapter $adapter): void
    {
        $adapter->createCollection(self::NOTES, [Attribute::string(key: 'title', size: 64)]);
    }

    private function notes(): Document
    {
        return new Document(['$id' => self::NOTES]);
    }

    private function idOf(mixed $value): ?string
    {
        return match (true) {
            $value instanceof Document => $value->getId(),
            \is_string($value) => $value,
            default => null,
        };
    }

    /**
     * @return list<string>
     */
    private function idsOf(mixed $documents): array
    {
        return \array_values(\array_filter(\array_map($this->idOf(...), \is_array($documents) ? $documents : []), \is_string(...)));
    }

    /**
     * @param  list<string>  $ids
     * @return list<string>
     */
    private function sorted(array $ids): array
    {
        \sort($ids);

        return $ids;
    }

    private function fakeClient(): Redis
    {
        $client = self::createStub(Redis::class);
        $client->method('ping')->willReturn(true);
        $client->method('multi')->willReturnCallback(function () use ($client): Redis {
            $this->pipelining = true;
            $this->queued = [];

            return $client;
        });
        $client->method('exec')->willReturnCallback(function (): mixed {
            $replies = $this->queued;
            $this->pipelining = false;
            $this->queued = [];

            return $replies;
        });
        $client->method('discard')->willReturnCallback(function (): bool {
            $this->pipelining = false;
            $this->queued = [];

            return true;
        });
        $client->method('get')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->strings[$key] ?? false));
        $client->method('mGet')->willReturnCallback(fn (mixed $keys): mixed => $this->reply($client, \array_map(
            fn (mixed $key): string|false => $this->strings[$this->text($key)] ?? false,
            \is_array($keys) ? \array_values($keys) : [],
        )));
        $client->method('set')->willReturnCallback(function (string $key, mixed $value) use ($client): mixed {
            $this->forget($key);
            $this->strings[$key] = $this->text($value);

            return $this->reply($client, true);
        });
        $client->method('incr')->willReturnCallback(function (string $key, int $by = 1) use ($client): mixed {
            $value = (int) ($this->strings[$key] ?? 0) + $by;
            $this->strings[$key] = (string) $value;

            return $this->reply($client, $value);
        });
        $client->method('exists')->willReturnCallback(fn (mixed ...$keys): mixed => $this->reply(
            $client,
            \count(\array_filter($keys, fn (mixed $key): bool => $this->has($this->text($key)))),
        ));
        $client->method('del')->willReturnCallback(function (mixed $key, mixed ...$otherKeys) use ($client): mixed {
            $removed = 0;
            foreach ([...(\is_array($key) ? \array_values($key) : [$key]), ...$otherKeys] as $candidate) {
                $candidate = $this->text($candidate);
                $removed += (int) $this->has($candidate);
                $this->forget($candidate);
            }

            return $this->reply($client, $removed);
        });
        $client->method('sAdd')->willReturnCallback(function (string $key, mixed ...$members) use ($client): mixed {
            $added = 0;
            foreach ($members as $member) {
                $member = $this->text($member);
                $added += (int) ! isset($this->sets[$key][$member]);
                $this->sets[$key][$member] = true;
            }

            return $this->reply($client, $added);
        });
        $client->method('sRem')->willReturnCallback(function (string $key, mixed ...$members) use ($client): mixed {
            $removed = 0;
            foreach ($members as $member) {
                $member = $this->text($member);
                $removed += (int) isset($this->sets[$key][$member]);
                unset($this->sets[$key][$member]);
            }
            if (($this->sets[$key] ?? null) === []) {
                unset($this->sets[$key]);
            }

            return $this->reply($client, $removed);
        });
        $client->method('sMembers')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->members($key)));
        $client->method('sIsMember')->willReturnCallback(fn (string $key, mixed $member): mixed => $this->reply($client, isset($this->sets[$key][$this->text($member)])));
        $client->method('sCard')->willReturnCallback(fn (string $key): mixed => $this->reply($client, \count($this->sets[$key] ?? [])));
        $client->method('sUnion')->willReturnCallback(fn (string ...$keys): mixed => $this->reply(
            $client,
            \array_values(\array_unique(\array_merge(...\array_map($this->members(...), $keys)))),
        ));
        $client->method('hSet')->willReturnCallback(function (string $key, string $field, mixed $value) use ($client): mixed {
            $this->hashWrites[] = $key.' '.$field;
            $added = (int) ! isset($this->hashes[$key][$field]);
            $this->hashes[$key][$field] = $this->text($value);

            return $this->reply($client, $added);
        });
        $client->method('hMSet')->willReturnCallback(function (string $key, mixed $fields) use ($client): mixed {
            foreach (\is_array($fields) ? $fields : [] as $field => $value) {
                $this->hashes[$key][(string) $field] = $this->text($value);
            }

            return $this->reply($client, true);
        });
        $client->method('hGet')->willReturnCallback(fn (string $key, string $field): mixed => $this->reply($client, $this->hashes[$key][$field] ?? false));
        $client->method('hGetAll')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->hashes[$key] ?? []));
        $client->method('hDel')->willReturnCallback(function (string $key, string ...$fields) use ($client): mixed {
            $removed = 0;
            foreach ($fields as $field) {
                $removed += (int) isset($this->hashes[$key][$field]);
                unset($this->hashes[$key][$field]);
            }
            if (($this->hashes[$key] ?? null) === []) {
                unset($this->hashes[$key]);
            }

            return $this->reply($client, $removed);
        });
        $client->method('rawCommand')->willThrowException(new \RedisException('MEMORY USAGE is not available'));
        $client->method('type')->willReturnCallback(fn (string $key): int => match (true) {
            isset($this->strings[$key]) => Redis::REDIS_STRING,
            isset($this->sets[$key]) => Redis::REDIS_SET,
            isset($this->hashes[$key]) => Redis::REDIS_HASH,
            default => Redis::REDIS_NOT_FOUND,
        });
        $client->method('scan')->willReturnCallback(fn (mixed $iterator, ?string $pattern = null): mixed => $this->keys($pattern ?? '*'));

        return $client;
    }

    private function reply(Redis $client, mixed $value): mixed
    {
        if (! $this->pipelining) {
            return $value;
        }
        $this->queued[] = $value;

        return $client;
    }

    private function text(mixed $value): string
    {
        return \is_scalar($value) ? (string) $value : '';
    }

    /**
     * @return list<string>
     */
    private function members(string $key): array
    {
        return \array_map($this->text(...), \array_keys($this->sets[$key] ?? []));
    }

    /**
     * @return list<string>
     */
    private function keys(string $pattern): array
    {
        $keys = \array_map($this->text(...), [...\array_keys($this->strings), ...\array_keys($this->sets), ...\array_keys($this->hashes)]);

        return \array_values(\array_filter($keys, static fn (string $key): bool => \fnmatch($pattern, $key)));
    }

    private function has(string $key): bool
    {
        return isset($this->strings[$key]) || isset($this->sets[$key]) || isset($this->hashes[$key]);
    }

    private function forget(string $key): void
    {
        unset($this->strings[$key], $this->sets[$key], $this->hashes[$key]);
    }

    /**
     * @return list<string>
     */
    private function recordedHashWrites(): array
    {
        return $this->hashWrites;
    }
}
