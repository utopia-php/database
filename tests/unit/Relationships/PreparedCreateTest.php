<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\RecordingLifecycle;
use Tests\Unit\Support\CountingMemory;
use Tests\Unit\Support\RelationshipMemory;
use Tests\Unit\Support\RelationshipSQLite;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Contention;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\Authorization;

/**
 * A create or update whose related documents are all new prepares them instead of reading each one first and back
 * after: with savepoints it writes them together at the end, without them each where it would have been written.
 * Each scenario runs once with the hook relating one by one as it always did, and once where it prepares, and
 * everything observable must match: what is stored, what is returned or thrown, the events and write hooks fired,
 * and what the write assigned to the documents handed in.
 */
final class PreparedCreateTest extends TestCase
{
    private const string ONE_BY_ONE = 'one by one';

    private const string DEFERRED = 'deferred';

    private const string ONE_BY_ONE_WITHOUT_SAVEPOINTS = 'one by one without savepoints';

    private const string IMMEDIATE = 'immediate';

    private const string GENERATED_ID = '/^[0-9a-f]{20}$/';

    private const array SHARED = ['shared tables' => 7, 'shared tables with an existing related document' => 7];

    /**
     * The scenarios that create only new related documents and succeed, so their related documents are prepared.
     */
    private const array PREPARED = [
        'one to many',
        'one to many past the maximum depth',
        'one to many one way',
        'many to many',
        'many to many past the maximum depth',
        'many to many one way',
        'many to one',
        'one to one',
        'one to one one way',
        'generated ids and own permissions',
        'documents mixed with ids',
        'one to many documents mixed with ids',
        'one to one by id',
        'existence checks skipped',
        'new document then its id',
        'one collection at several depths',
        'inside a transaction',
        'created through an update',
        'one to many update',
        'many to many update',
        'many to one update',
        'shared tables',
    ];

    private const array TENANT_PER_DOCUMENT = ['tenant per document', 'missing tenant on a related document'];

    /**
     * What a write assigns to the documents handed to it. Written one by one, each related document is also left
     * populated and decoded, as createDocument() leaves the document it is given; prepared, it is left as written.
     */
    private const array ASSIGNED = ['$id', '$sequence', '$collection', '$permissions'];

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function scenarios(): iterable
    {
        foreach (['memory', 'sqlite'] as $engine) {
            foreach (\array_keys(self::writes()) as $scenario) {
                yield $engine.': '.$scenario => [$engine, $scenario];
            }
        }
    }

    #[DataProvider('scenarios')]
    public function testDeferredCreateMatchesOneByOne(string $engine, string $scenario): void
    {
        $this->assertSameObservations($this->observe($engine, self::ONE_BY_ONE, $scenario), $this->observe($engine, self::DEFERRED, $scenario));
    }

    /**
     * Written one by one or each where it would be written, a write that fails restores the related documents it was
     * given, so a transaction that retries the write starts over from them.
     */
    #[DataProvider('scenarios')]
    public function testImmediateCreateMatchesOneByOne(string $engine, string $scenario): void
    {
        $oneByOne = $this->observe($engine, self::ONE_BY_ONE_WITHOUT_SAVEPOINTS, $scenario);
        $immediate = $this->observe($engine, self::IMMEDIATE, $scenario);

        if (isset($immediate['thrown'])) {
            foreach ([$oneByOne, $immediate] as $observation) {
                $given = $observation['given'];
                $inputs = $observation['inputs'];
                if (! \is_array($given) || ! \is_array($inputs)) {
                    $this->fail('An observation lacks the documents handed in');
                }
                $this->assertSame(\array_slice($given, 1), \array_slice($inputs, 1), 'A failed write left the related documents it was given changed');
            }
        }

        $this->assertSameObservations($oneByOne, $immediate);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function preparedScenarios(): iterable
    {
        foreach (self::PREPARED as $scenario) {
            yield $scenario => [$scenario];
        }
    }

    #[DataProvider('preparedScenarios')]
    public function testScenarioPreparesItsRelatedDocuments(string $scenario): void
    {
        foreach ([self::DEFERRED => self::ONE_BY_ONE, self::IMMEDIATE => self::ONE_BY_ONE_WITHOUT_SAVEPOINTS] as $prepared => $oneByOne) {
            $this->assertLessThan(
                $this->observe('memory', $oneByOne, $scenario)['reads'],
                $this->observe('memory', $prepared, $scenario)['reads'],
                'The '.$prepared.' write related its documents one by one, so the scenario does not compare it',
            );
        }
    }

    public function testPreparedCreateReadsNoRelatedDocumentBeforeOrAfterWritingIt(): void
    {
        foreach ([self::ONE_BY_ONE, self::DEFERRED, self::ONE_BY_ONE_WITHOUT_SAVEPOINTS, self::IMMEDIATE] as $mode) {
            $database = $this->database('memory', $mode);
            self::chain($database, RelationshipType::OneToMany, 2);
            $adapter = $database->getAdapter();
            $this->assertInstanceOf(CountingMemory::class, $adapter);
            $adapter->reset();

            $database->createDocument('level0', self::tree(RelationshipType::OneToMany, 'root', 0, 2));

            if ($mode === self::DEFERRED || $mode === self::IMMEDIATE) {
                $this->assertSame(0, $adapter->documentReads, 'A '.$mode.' create read a related document');
            } else {
                $this->assertGreaterThan(0, $adapter->documentReads, 'Relating one by one reads each related document');
            }
        }
    }

    public function testAFilterIsAppliedOnceWhenAnAssociativeValueHoldsAStoredDocument(): void
    {
        $filters = ['wrap' => [
            'encode' => static fn (mixed $value): mixed => \is_string($value) ? '['.$value.']' : $value,
            'decode' => static fn (mixed $value): mixed => \is_string($value) && \str_starts_with($value, '[') ? \substr($value, 1, -1) : $value,
        ]];

        foreach (['memory', 'sqlite'] as $engine) {
            foreach ([self::ONE_BY_ONE, self::DEFERRED] as $mode) {
                $database = $this->database($engine, $mode, filters: $filters);
                foreach (['root', 'mid', 'side'] as $collection) {
                    $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::string(key: 'name', size: 64)], permissions: self::permissions(), documentSecurity: true));
                }
                $database->createCollection(Collection::create(id: 'leaf', attributes: [Attribute::string(key: 'name', size: 64, filters: ['wrap'])], permissions: self::permissions(), documentSecurity: true));
                $database->createRelationship('root', Relationship::manyToOne(relatedCollection: 'mid', twoWay: true, key: 'mid', twoWayKey: 'roots', onDelete: RelationshipDeleteAction::Cascade));
                $database->createRelationship('root', Relationship::manyToOne(relatedCollection: 'side', twoWay: true, key: 'side', twoWayKey: 'roots', onDelete: RelationshipDeleteAction::Cascade));
                $database->createRelationship('mid', Relationship::oneToMany(relatedCollection: 'leaf', twoWay: true, key: 'leaves', twoWayKey: 'mid', onDelete: RelationshipDeleteAction::Cascade));
                $database->createDocument('leaf', new Document(['$id' => 'existing', 'name' => 'old']));

                $database->createDocument('root', new Document([
                    '$id' => 'root',
                    'side' => new Document(['name' => 'side']),
                    'mid' => ['name' => 'mid', 'leaves' => [new Document(['$id' => 'k1', 'name' => 'k1']), new Document(['$id' => 'existing', 'name' => 'new'])]],
                ]));

                $names = \array_map(
                    static fn (string $id): mixed => $database->skipRelationships(static fn (): Document => $database->getDocument('leaf', $id))->getAttribute('name'),
                    ['k1', 'existing'],
                );
                $this->assertSame(['k1', 'new'], $names, 'Relating '.$mode.' on '.$engine.' encoded a name more than once');
            }
        }
    }

    /**
     * @return iterable<string, array{bool}>
     */
    public static function preparing(): iterable
    {
        yield 'prepared' => [true];
        yield 'one by one' => [false];
    }

    #[DataProvider('preparing')]
    public function testALockConflictWhileWritingPreparedDocumentsIsRetried(bool $prepare): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends RelationshipSQLite {
            private bool $conflicted = false;

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                if (! $this->conflicted && $collection->getId() === 'children') {
                    $this->conflicted = true;
                    $this->getPDO()->exec('ROLLBACK');

                    throw new Contention('Deadlock found when trying to get lock');
                }

                return parent::createDocument($collection, $document);
            }
        };
        $database = $this->family($adapter, $prepare);

        $database->createDocument('parents', self::parent());

        $this->assertFamilyStored($database);
    }

    /**
     * A lock wait that times out without the engine rolling the transaction back leaves the savepoint to roll back,
     * but the conflicting lock is held until the whole transaction rolls back, so relating one by one in the same
     * transaction would only wait for it again.
     */
    #[DataProvider('preparing')]
    public function testALockConflictTheSavepointSurvivesIsLeftToTheTransactionRetry(bool $prepare): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends RelationshipSQLite {
            public int $lockWaits = 0;

            public bool $locked = false;

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                if ($this->locked && $collection->getId() === 'children') {
                    $this->lockWaits++;

                    throw new Contention('Lock wait timeout exceeded; try restarting transaction');
                }

                return parent::createDocument($collection, $document);
            }

            #[\Override]
            public function rollbackTransaction(): bool
            {
                $rolledBack = parent::rollbackTransaction();
                if (! $this->inTransaction()) {
                    $this->locked = false;
                }

                return $rolledBack;
            }
        };
        $database = $this->family($adapter, $prepare);
        $adapter->locked = true;

        $database->createDocument('parents', self::parent());

        $this->assertFamilyStored($database);
        $this->assertSame(1, $adapter->lockWaits, 'A write waited for a lock its own transaction still held');
    }

    public function testADocumentWhosePreparedSavepointFailedToCommitIsRelatedAgainOnRetry(): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends RelationshipSQLite {
            public bool $failSavepointCommit = false;

            #[\Override]
            public function commitTransaction(): bool
            {
                if ($this->failSavepointCommit && $this->inTransaction > 1) {
                    $this->failSavepointCommit = false;
                    $this->getPDO()->exec('ROLLBACK');
                    $this->inTransaction = 0;

                    throw new Contention('Deadlock found when trying to get lock');
                }

                return parent::commitTransaction();
            }
        };
        $database = $this->family($adapter, true);
        $adapter->failSavepointCommit = true;

        $database->createDocument('parents', self::parent());

        $this->assertFalse($adapter->failSavepointCommit, 'The savepoint commit never failed');
        $this->assertFamilyStored($database);
    }

    private function family(RelationshipSQLite $adapter, bool $prepare): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization($authorization)->setDatabase('prepared_create')->setNamespace('prepared');
        $database->create();
        $database->addHook(new Relationships($database, prepare: $prepare));
        foreach (['parents', 'children'] as $collection) {
            $database->createCollection(Collection::create(id: $collection, attributes: [Attribute::string(key: 'name', size: 64)], permissions: self::permissions(), documentSecurity: false));
        }
        $database->createRelationship('parents', Relationship::oneToMany(relatedCollection: 'children', twoWay: true, key: 'children', twoWayKey: 'parent'));

        return $database;
    }

    private static function parent(): Document
    {
        return new Document(['$id' => 'p1', 'name' => 'p1', 'children' => [new Document(['$id' => 'c1', 'name' => 'c1'])]]);
    }

    private function assertFamilyStored(Database $database): void
    {
        $parents = $database->skipRelationships(static fn (): array => $database->find('parents'));
        $children = $database->skipRelationships(static fn (): array => $database->find('children'));

        $this->assertSame(['p1'], \array_map(static fn (Document $parent): string => $parent->getId(), $parents));
        $this->assertSame(
            [['c1', 'p1']],
            \array_map(static fn (Document $child): array => [$child->getId(), $child->getAttribute('parent')], $children),
        );
    }

    /**
     * @param  array<mixed>  $expected
     * @param  array<mixed>  $actual
     */
    private function assertSameObservations(array $expected, array $actual): void
    {
        unset($expected['reads'], $actual['reads']);

        $this->assertSame($expected, $actual);
    }

    /**
     * Each scenario sets up its schema and data, and returns the document it writes with the write.
     *
     * @return array<string, Closure(Database): array{Document, Closure(Document): mixed}>
     */
    private static function writes(): array
    {
        $create = static fn (Database $database, string $collection): Closure => static fn (Document $document): Document => $database->createDocument($collection, $document);
        $tree = static fn (RelationshipType $type, int $depth, bool $twoWay = true): Closure => static function (Database $database) use ($type, $depth, $twoWay, $create): array {
            self::chain($database, $type, $depth, $twoWay);

            return [self::tree($type, 'root', 0, $depth), $create($database, 'level0')];
        };
        $nodes = static function (Database $database): void {
            $database->createCollection(Collection::create(id: 'node', attributes: [Attribute::string(key: 'name', size: 64)], permissions: self::permissions(), documentSecurity: true));
            $database->createCollection(Collection::create(id: 'tag', attributes: [Attribute::string(key: 'name', size: 64)], permissions: self::permissions(), documentSecurity: true));
            $database->createRelationship('node', Relationship::oneToMany(relatedCollection: 'tag', twoWay: true, key: 'tags', twoWayKey: 'node', onDelete: RelationshipDeleteAction::Cascade));
            $database->createRelationship('tag', Relationship::manyToMany(relatedCollection: 'node', twoWay: true, key: 'nodes', twoWayKey: 'labels', onDelete: RelationshipDeleteAction::Cascade));
        };

        return [
            'one to many' => $tree(RelationshipType::OneToMany, 2),
            'one to many past the maximum depth' => $tree(RelationshipType::OneToMany, 4),
            'one to many one way' => $tree(RelationshipType::OneToMany, 2, false),
            'many to many' => $tree(RelationshipType::ManyToMany, 2),
            'many to many past the maximum depth' => $tree(RelationshipType::ManyToMany, 4),
            'many to many one way' => $tree(RelationshipType::ManyToMany, 2, false),
            'many to one' => $tree(RelationshipType::ManyToOne, 3),
            'one to one' => $tree(RelationshipType::OneToOne, 3),
            'one to one one way' => $tree(RelationshipType::OneToOne, 3, false),
            'generated ids and own permissions' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
                    'next' => [
                        new Document(['name' => 'first', 'next' => [new Document(['name' => 'deep'])]]),
                        new Document(['name' => 'second', '$permissions' => [Permission::read(Role::user('someone'))]]),
                    ],
                ]), $create($database, 'level0')];
            },
            'documents mixed with ids' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToMany, 2);
                $database->createDocument('level1', new Document(['$id' => 'existing', 'name' => 'existing']));
                $database->createDocument('level2', new Document(['$id' => 'leaf', 'name' => 'leaf']));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => ['leaf', new Document(['$id' => 'a1', 'name' => 'a1'])]]),
                        'existing',
                        new Document(['$id' => 'b', 'name' => 'b']),
                    ],
                ]), $create($database, 'level0')];
            },
            'one to many documents mixed with ids' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level1', new Document(['$id' => 'existing', 'name' => 'existing']));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1'])]]),
                        'existing',
                        new Document(['$id' => 'b', 'name' => 'b']),
                    ],
                ]), $create($database, 'level0')];
            },
            'existing related documents' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level1', new Document(['$id' => 'same', 'name' => 'same', 'score' => 1, '$permissions' => [Permission::read(Role::any())]]));
                $database->createDocument('level1', new Document(['$id' => 'changed', 'name' => 'before', 'score' => 1]));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => [
                        new Document(['$id' => 'new', 'name' => 'new', 'next' => [new Document(['$id' => 'leaf', 'name' => 'leaf'])]]),
                        new Document(['$id' => 'same', 'name' => 'same', 'score' => 1, '$permissions' => [Permission::read(Role::any())]]),
                        new Document(['$id' => 'changed', 'name' => 'after', 'next' => [new Document(['$id' => 'under', 'name' => 'under'])]]),
                    ],
                ]), $create($database, 'level0')];
            },
            'existing many to many related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToMany, 2);
                $database->createDocument('level1', new Document(['$id' => 'shared', 'name' => 'shared']));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => [
                        new Document(['$id' => 'new', 'name' => 'new']),
                        new Document(['$id' => 'shared', 'name' => 'renamed']),
                    ],
                ]), $create($database, 'level0')];
            },
            'associative related document holding a stored one' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToOne, 2);
                $database->createDocument('level2', new Document(['$id' => 'existing', 'name' => 'before']));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => ['name' => 'mid', 'next' => new Document(['$id' => 'existing', 'name' => 'after'])],
                ]), $create($database, 'level0')];
            },
            'repeated related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => [
                        new Document(['$id' => 'twice', 'name' => 'twice']),
                        new Document(['$id' => 'twice', 'name' => 'twice']),
                    ],
                ]), $create($database, 'level0')];
            },
            'one to one by id' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToOne, 2);
                $database->createDocument('level2', new Document(['$id' => 'leaf', 'name' => 'leaf']));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => new Document(['$id' => 'a', 'name' => 'a', 'next' => 'leaf']),
                ]), $create($database, 'level0')];
            },
            'existence checks skipped' => static function (Database $database): array {
                self::chain($database, RelationshipType::OneToOne, 2);

                return [
                    self::tree(RelationshipType::OneToOne, 'root', 0, 2),
                    static fn (Document $document): Document => $database->skipRelationshipsExistCheck(static fn (): Document => $database->createDocument('level0', $document)),
                ];
            },
            'new document then its id' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1']), 'a1']]),
                        'a',
                    ],
                ]), $create($database, 'level0')];
            },
            'one collection at several depths' => static function (Database $database) use ($create, $nodes): array {
                $nodes($database);

                return [new Document([
                    '$id' => 'n0',
                    'name' => 'n0',
                    'tags' => [
                        new Document(['$id' => 't1', 'name' => 't1', 'nodes' => [new Document(['$id' => 'n1', 'name' => 'n1']), new Document(['$id' => 'n2', 'name' => 'n2'])]]),
                        new Document(['$id' => 't2', 'name' => 't2', 'nodes' => [new Document(['$id' => 'n3', 'name' => 'n3'])]]),
                    ],
                ]), $create($database, 'node')];
            },
            'id already taken' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToOne, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'taken']));

                return [self::tree(RelationshipType::OneToOne, 'root', 0, 2), $create($database, 'level0')];
            },
            'related document with the id of the document it belongs to' => static function (Database $database) use ($create, $nodes): array {
                $nodes($database);

                return [new Document([
                    '$id' => 'n0',
                    'tags' => [new Document(['$id' => 't1', 'nodes' => [new Document(['$id' => 'n1']), new Document(['$id' => 'n0'])]])],
                ]), $create($database, 'node')];
            },
            'inside a transaction' => static function (Database $database): array {
                self::chain($database, RelationshipType::ManyToMany, 2);

                return [
                    self::tree(RelationshipType::ManyToMany, 'root', 0, 2),
                    static fn (Document $document): Document => $database->withTransaction(static fn (): Document => $database->createDocument('level0', $document)),
                ];
            },
            'created through an update' => static function (Database $database): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root']));

                return [new Document([
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1']), new Document(['$id' => 'a2', 'name' => 'a2'])]]),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'one to many update' => static function (Database $database): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root', 'next' => [
                    new Document(['$id' => 'kept', 'name' => 'kept']),
                    new Document(['$id' => 'dropped', 'name' => 'dropped']),
                ]]));
                $database->createDocument('level1', new Document(['$id' => 'loose', 'name' => 'loose']));

                return [new Document([
                    'name' => 'renamed',
                    'next' => [
                        'kept',
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1']), new Document(['$id' => 'a2', 'name' => 'a2'])]]),
                        'loose',
                        new Document(['$id' => 'b', 'name' => 'b']),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'many to many update' => static function (Database $database): array {
                self::chain($database, RelationshipType::ManyToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root', 'next' => [
                    new Document(['$id' => 'kept', 'name' => 'kept']),
                    new Document(['$id' => 'dropped', 'name' => 'dropped']),
                ]]));
                $database->createDocument('level1', new Document(['$id' => 'loose', 'name' => 'loose']));

                return [new Document([
                    'next' => [
                        'kept',
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1'])]]),
                        'loose',
                        new Document(['$id' => 'b', 'name' => 'b']),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'many to one update' => static function (Database $database): array {
                self::chain($database, RelationshipType::ManyToOne, 2);
                $database->createDocument('level1', new Document(['$id' => 'shared', 'name' => 'shared']));
                $database->createDocument('level0', new Document(['$id' => 'old', 'name' => 'old', 'next' => 'shared']));

                return [new Document([
                    'prev' => [
                        'old',
                        new Document(['$id' => 'p1', 'name' => 'p1']),
                        new Document(['$id' => 'p2', 'name' => 'p2']),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level1', 'shared', $document)];
            },
            'update with an existing related document' => static function (Database $database): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root']));
                $database->createDocument('level1', new Document(['$id' => 'loose', 'name' => 'before']));

                return [new Document([
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a']),
                        new Document(['$id' => 'loose', 'name' => 'after']),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'update with an invalid related document' => static function (Database $database): array {
                self::chain($database, RelationshipType::ManyToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root']));

                return [new Document([
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1'])]]),
                        new Document(['$id' => 'b', 'score' => 'many']),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'shared tables' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToMany, 2);

                return [self::tree(RelationshipType::ManyToMany, 'root', 0, 2), $create($database, 'level0')];
            },
            'shared tables with an existing related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level2', new Document(['$id' => 'existing', 'name' => 'before']));

                return [new Document([
                    '$id' => 'root',
                    'next' => [new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1']), new Document(['$id' => 'existing', 'name' => 'after'])]])],
                ]), $create($database, 'level0')];
            },
            'tenant per document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $tenant = static fn (string $id, array $next = []): Document => new Document(['$id' => $id, 'name' => $id, '$tenant' => 7] + ($next === [] ? [] : ['next' => $next]));

                return [$tenant('root', [$tenant('a', [$tenant('a1')]), $tenant('b')]), $create($database, 'level0')];
            },
            'missing tenant on a related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    '$tenant' => 7,
                    'next' => [new Document(['$id' => 'a', '$tenant' => 7, 'next' => [new Document(['$id' => 'a1'])]])],
                ]), $create($database, 'level0')];
            },
            'invalid related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1']), new Document(['$id' => 'a2', 'score' => 'many'])]]),
                        new Document(['$id' => 'b', 'name' => 'b', 'score' => 'many']),
                    ],
                ]), $create($database, 'level0')];
            },
            'invalid relationship value' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1'])]]),
                        new Document(['$id' => 'b', 'next' => [7]]),
                    ],
                ]), $create($database, 'level0')];
            },
            'related collection the caller may not create in' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->updateCollection('level2', new CollectionUpdate(permissions: [Permission::read(Role::any())], documentSecurity: true));

                return [self::tree(RelationshipType::OneToMany, 'root', 0, 2), $create($database, 'level0')];
            },
            'many to many link the caller may not update' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::ManyToMany, 2);
                $database->updateCollection('level2', new CollectionUpdate(permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: true));
                $database->createDocument('level2', new Document(['$id' => 'locked', 'name' => 'locked', '$permissions' => [Permission::read(Role::any())]]));

                return [new Document([
                    '$id' => 'root',
                    'next' => [new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1']), new Document(['$id' => 'locked'])]])],
                ]), $create($database, 'level0')];
            },
            'unreadable existing related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createDocument('level2', new Document(['$id' => 'hidden', 'name' => 'hidden', '$permissions' => []]));

                return [new Document([
                    '$id' => 'root',
                    'next' => [new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1']), new Document(['$id' => 'hidden', 'name' => 'mine'])]])],
                ]), $create($database, 'level0')];
            },
            'unique attribute shared by related documents' => static function (Database $database) use ($create): array {
                self::chain($database, RelationshipType::OneToMany, 2);
                $database->createIndex('level2', Index::unique('name', ['name']));

                return [new Document([
                    '$id' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'same'])]]),
                        new Document(['$id' => 'b', 'score' => 'many', 'next' => [new Document(['$id' => 'b1', 'name' => 'same'])]]),
                    ],
                ]), $create($database, 'level0')];
            },
        ];
    }

    /**
     * @return array<mixed>
     */
    private function observe(string $engine, string $mode, string $scenario): array
    {
        $database = $this->database($engine, $mode, self::SHARED[$scenario] ?? null, \in_array($scenario, self::TENANT_PER_DOCUMENT, true));
        [$document, $write] = self::writes()[$scenario]($database);

        $lifecycle = new RecordingLifecycle();
        $writes = new RecordingWrite();
        $database->addHook($lifecycle);
        $database->addHook($writes);

        $inputs = [];
        $this->collect($document, $inputs);
        $assigned = static fn (Document $input): array => \array_intersect_key($input->getArrayCopy(), \array_flip(self::ASSIGNED));
        $given = \array_map($assigned, $inputs);

        $adapter = $database->getAdapter();
        if ($adapter instanceof CountingMemory) {
            $adapter->reset();
        }

        try {
            $result = $write($document);
            $outcome = ['returned' => $result instanceof Document ? $this->export($result) : $result];
        } catch (Throwable $error) {
            $outcome = ['thrown' => [$error::class, $error->getMessage()]];
        }

        return $this->normalize($outcome + [
            'given' => $given,
            'inputs' => \array_map($assigned, $inputs),
            'stored' => $this->stored($database),
            'events' => \array_map(
                static fn (Event $event): string => $event->name,
                $lifecycle->getEvents(),
            ),
            'writes' => $writes->writes,
            'reads' => $adapter instanceof CountingMemory ? $adapter->documentReads : null,
        ]);
    }

    /**
     * @param  list<Document>  $documents
     */
    private function collect(Document $document, array &$documents): void
    {
        $documents[] = $document;
        foreach ((array) $document as $value) {
            foreach (\is_array($value) ? $value : [$value] as $item) {
                if ($item instanceof Document) {
                    $this->collect($item, $documents);
                }
            }
        }
    }

    /**
     * @param  array<string, array{encode: callable, decode: callable}>  $filters
     */
    private function database(string $engine, string $mode, ?int $tenant = null, bool $tenantPerDocument = false, array $filters = []): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $savepoints = $mode === self::ONE_BY_ONE || $mode === self::DEFERRED;
        $adapter = $engine === 'memory'
            ? new RelationshipMemory($savepoints)
            : new RelationshipSQLite(new PDO('sqlite::memory:'), $savepoints);

        $database = new Database($adapter, new Cache(new None()), $filters);
        $database
            ->setAuthorization($authorization)
            ->setDatabase('prepared_create')
            ->setNamespace('prepared');

        if ($tenant !== null || $tenantPerDocument) {
            $database
                ->setSharedTables(true)
                ->setTenantPerDocument($tenantPerDocument)
                ->setTenant($tenant);
        }

        $database->create();
        $database->addHook(new Relationships($database, prepare: $mode === self::DEFERRED || $mode === self::IMMEDIATE));
        $database->addHook(new Permissions());

        return $database;
    }

    private static function chain(Database $database, RelationshipType $type, int $depth, bool $twoWay = true): void
    {
        for ($level = 0; $level <= $depth; $level++) {
            $database->createCollection(Collection::create(
                id: 'level'.$level,
                attributes: [Attribute::string(key: 'name', size: 64), Attribute::integer(key: 'score')],
                permissions: self::permissions(),
                documentSecurity: true,
            ));
        }

        for ($level = 0; $level < $depth; $level++) {
            $database->createRelationship('level'.$level, Relationship::fromArray([
                'relatedCollection' => 'level'.($level + 1),
                'relationType' => $type,
                'twoWay' => $twoWay,
                'key' => 'next',
                'twoWayKey' => 'prev',
                'onDelete' => RelationshipDeleteAction::Cascade,
            ]));
        }
    }

    private static function tree(RelationshipType $type, string $id, int $level, int $depth): Document
    {
        $node = [
            '$id' => $id,
            'name' => 'node '.$id,
            'score' => $level,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
        ];

        if ($level < $depth) {
            $node['next'] = match ($type) {
                RelationshipType::ManyToOne, RelationshipType::OneToOne => self::tree($type, $id.'_0', $level + 1, $depth),
                default => [self::tree($type, $id.'_0', $level + 1, $depth), self::tree($type, $id.'_1', $level + 1, $depth)],
            };
        }

        return new Document($node);
    }

    /**
     * @return array<string>
     */
    private static function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
    }

    /**
     * @return array<string, list<array<mixed>>>
     */
    private function stored(Database $database): array
    {
        return $database->getAuthorization()->skip(fn (): array => $database->skipRelationships(function () use ($database): array {
            $collections = \array_map(static fn (Document $collection): string => $collection->getId(), $database->listCollections(100));
            \sort($collections);

            $stored = [];
            foreach ($collections as $collection) {
                $rows = $database->find($collection, [Query::limit(1000), Query::orderAsc('$sequence')]);
                $stored[$collection] = \array_values(\array_map($this->export(...), $rows));
            }

            return $stored;
        }));
    }

    /**
     * @return array<mixed>
     */
    private function export(Document $document): array
    {
        return $this->withoutTimestamps($document->getArrayCopy());
    }

    /**
     * @param  array<mixed>  $values
     * @return array<mixed>
     */
    private function withoutTimestamps(array $values): array
    {
        unset($values['$createdAt'], $values['$updatedAt']);

        foreach ($values as $key => $value) {
            if (\is_array($value)) {
                $values[$key] = $this->withoutTimestamps($value);
            }
        }

        return $values;
    }

    /**
     * Generated ids differ from run to run, so each is replaced by the order it first appears in.
     *
     * @param  array<mixed>  $values
     * @param  array<string, string>  $ids
     * @return array<mixed>
     */
    private function normalize(array $values, array &$ids = []): array
    {
        foreach ($values as $key => $value) {
            if (\is_array($value)) {
                $values[$key] = $this->normalize($value, $ids);
            } elseif (\is_string($value) && \preg_match(self::GENERATED_ID, $value) === 1) {
                $values[$key] = $ids[$value] ??= 'generated-'.\count($ids);
            }
        }

        return $values;
    }
}
