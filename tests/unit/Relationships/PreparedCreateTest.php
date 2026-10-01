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
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ForeignKeyAction;

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
     * Written one by one, a write that fails leaves the related documents it got to as it changed them, and a
     * transaction that retries the write relates whatever is left of them; written where each would be written,
     * a write that fails restores the related documents it was given, so a retry starts over from them.
     */
    #[DataProvider('scenarios')]
    public function testImmediateCreateMatchesOneByOne(string $engine, string $scenario): void
    {
        $oneByOne = $this->observe($engine, self::ONE_BY_ONE_WITHOUT_SAVEPOINTS, $scenario);
        $immediate = $this->observe($engine, self::IMMEDIATE, $scenario);

        if (isset($immediate['thrown']) && $immediate['inputs'] !== $oneByOne['inputs']) {
            $this->assertSame(\array_slice($immediate['given'], 1), \array_slice($immediate['inputs'], 1), 'A failed write left the related documents it was given changed');
            unset($oneByOne['inputs'], $immediate['inputs']);
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
            self::chain($database, RelationType::OneToMany, 2);
            $adapter = $database->getAdapter();
            $this->assertInstanceOf(CountingMemory::class, $adapter);
            $adapter->reset();

            $database->createDocument('level0', self::tree(RelationType::OneToMany, 'root', 0, 2));

            if ($mode === self::DEFERRED || $mode === self::IMMEDIATE) {
                $this->assertSame(0, $adapter->documentReads, 'A '.$mode.' create read a related document');
            } else {
                $this->assertGreaterThan(0, $adapter->documentReads, 'Relating one by one reads each related document');
            }
        }
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
        $tree = static fn (RelationType $type, int $depth, bool $twoWay = true): Closure => static function (Database $database) use ($type, $depth, $twoWay, $create): array {
            self::chain($database, $type, $depth, $twoWay);

            return [self::tree($type, 'root', 0, $depth), $create($database, 'level0')];
        };
        $nodes = static function (Database $database): void {
            $database->createCollection(new Collection(id: 'node', attributes: [Attribute::string(key: 'name', size: 64)], permissions: self::permissions(), documentSecurity: true));
            $database->createCollection(new Collection(id: 'tag', attributes: [Attribute::string(key: 'name', size: 64)], permissions: self::permissions(), documentSecurity: true));
            $database->createRelationship(Relationship::oneToMany(collection: 'node', relatedCollection: 'tag', twoWay: true, key: 'tags', twoWayKey: 'node', onDelete: ForeignKeyAction::Cascade));
            $database->createRelationship(Relationship::manyToMany(collection: 'tag', relatedCollection: 'node', twoWay: true, key: 'nodes', twoWayKey: 'labels', onDelete: ForeignKeyAction::Cascade));
        };

        return [
            'one to many' => $tree(RelationType::OneToMany, 2),
            'one to many past the maximum depth' => $tree(RelationType::OneToMany, 4),
            'one to many one way' => $tree(RelationType::OneToMany, 2, false),
            'many to many' => $tree(RelationType::ManyToMany, 2),
            'many to many past the maximum depth' => $tree(RelationType::ManyToMany, 4),
            'many to many one way' => $tree(RelationType::ManyToMany, 2, false),
            'many to one' => $tree(RelationType::ManyToOne, 3),
            'one to one' => $tree(RelationType::OneToOne, 3),
            'one to one one way' => $tree(RelationType::OneToOne, 3, false),
            'generated ids and own permissions' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);

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
                self::chain($database, RelationType::ManyToMany, 2);
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
                self::chain($database, RelationType::OneToMany, 2);
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
                self::chain($database, RelationType::OneToMany, 2);
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
                self::chain($database, RelationType::ManyToMany, 2);
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
            'repeated related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::ManyToMany, 2);

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
                self::chain($database, RelationType::OneToOne, 2);
                $database->createDocument('level2', new Document(['$id' => 'leaf', 'name' => 'leaf']));

                return [new Document([
                    '$id' => 'root',
                    'name' => 'root',
                    'next' => new Document(['$id' => 'a', 'name' => 'a', 'next' => 'leaf']),
                ]), $create($database, 'level0')];
            },
            'existence checks skipped' => static function (Database $database): array {
                self::chain($database, RelationType::OneToOne, 2);

                return [
                    self::tree(RelationType::OneToOne, 'root', 0, 2),
                    static fn (Document $document): Document => $database->skipRelationshipsExistCheck(static fn (): Document => $database->createDocument('level0', $document)),
                ];
            },
            'new document then its id' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::ManyToMany, 2);

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
                self::chain($database, RelationType::OneToOne, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'taken']));

                return [self::tree(RelationType::OneToOne, 'root', 0, 2), $create($database, 'level0')];
            },
            'related document with the id of the document it belongs to' => static function (Database $database) use ($create, $nodes): array {
                $nodes($database);

                return [new Document([
                    '$id' => 'n0',
                    'tags' => [new Document(['$id' => 't1', 'nodes' => [new Document(['$id' => 'n1']), new Document(['$id' => 'n0'])]])],
                ]), $create($database, 'node')];
            },
            'inside a transaction' => static function (Database $database): array {
                self::chain($database, RelationType::ManyToMany, 2);

                return [
                    self::tree(RelationType::ManyToMany, 'root', 0, 2),
                    static fn (Document $document): Document => $database->withTransaction(static fn (): Document => $database->createDocument('level0', $document)),
                ];
            },
            'created through an update' => static function (Database $database): array {
                self::chain($database, RelationType::OneToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root']));

                return [new Document([
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1']), new Document(['$id' => 'a2', 'name' => 'a2'])]]),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'one to many update' => static function (Database $database): array {
                self::chain($database, RelationType::OneToMany, 2);
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
                self::chain($database, RelationType::ManyToMany, 2);
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
                self::chain($database, RelationType::ManyToOne, 2);
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
                self::chain($database, RelationType::OneToMany, 2);
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
                self::chain($database, RelationType::ManyToMany, 2);
                $database->createDocument('level0', new Document(['$id' => 'root', 'name' => 'root']));

                return [new Document([
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1'])]]),
                        new Document(['$id' => 'b', 'score' => 'many']),
                    ],
                ]), static fn (Document $document): Document => $database->updateDocument('level0', 'root', $document)];
            },
            'shared tables' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::ManyToMany, 2);

                return [self::tree(RelationType::ManyToMany, 'root', 0, 2), $create($database, 'level0')];
            },
            'shared tables with an existing related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);
                $database->createDocument('level2', new Document(['$id' => 'existing', 'name' => 'before']));

                return [new Document([
                    '$id' => 'root',
                    'next' => [new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1']), new Document(['$id' => 'existing', 'name' => 'after'])]])],
                ]), $create($database, 'level0')];
            },
            'tenant per document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);
                $tenant = static fn (string $id, array $next = []): Document => new Document(['$id' => $id, 'name' => $id, '$tenant' => 7] + ($next === [] ? [] : ['next' => $next]));

                return [$tenant('root', [$tenant('a', [$tenant('a1')]), $tenant('b')]), $create($database, 'level0')];
            },
            'missing tenant on a related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    '$tenant' => 7,
                    'next' => [new Document(['$id' => 'a', '$tenant' => 7, 'next' => [new Document(['$id' => 'a1'])]])],
                ]), $create($database, 'level0')];
            },
            'invalid related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'name' => 'a', 'next' => [new Document(['$id' => 'a1', 'name' => 'a1']), new Document(['$id' => 'a2', 'score' => 'many'])]]),
                        new Document(['$id' => 'b', 'name' => 'b', 'score' => 'many']),
                    ],
                ]), $create($database, 'level0')];
            },
            'invalid relationship value' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);

                return [new Document([
                    '$id' => 'root',
                    'next' => [
                        new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1'])]]),
                        new Document(['$id' => 'b', 'next' => [7]]),
                    ],
                ]), $create($database, 'level0')];
            },
            'related collection the caller may not create in' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);
                $database->updateCollection('level2', [Permission::read(Role::any())], true);

                return [self::tree(RelationType::OneToMany, 'root', 0, 2), $create($database, 'level0')];
            },
            'many to many link the caller may not update' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::ManyToMany, 2);
                $database->updateCollection('level2', [Permission::create(Role::any()), Permission::read(Role::any())], true);
                $database->createDocument('level2', new Document(['$id' => 'locked', 'name' => 'locked', '$permissions' => [Permission::read(Role::any())]]));

                return [new Document([
                    '$id' => 'root',
                    'next' => [new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1']), new Document(['$id' => 'locked'])]])],
                ]), $create($database, 'level0')];
            },
            'unreadable existing related document' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);
                $database->createDocument('level2', new Document(['$id' => 'hidden', 'name' => 'hidden', '$permissions' => []]));

                return [new Document([
                    '$id' => 'root',
                    'next' => [new Document(['$id' => 'a', 'next' => [new Document(['$id' => 'a1']), new Document(['$id' => 'hidden', 'name' => 'mine'])]])],
                ]), $create($database, 'level0')];
            },
            'unique attribute shared by related documents' => static function (Database $database) use ($create): array {
                self::chain($database, RelationType::OneToMany, 2);
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

    private function database(string $engine, string $mode, ?int $tenant = null, bool $tenantPerDocument = false): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $savepoints = $mode === self::ONE_BY_ONE || $mode === self::DEFERRED;
        $adapter = $engine === 'memory'
            ? new RelationshipMemory($savepoints)
            : new RelationshipSQLite(new PDO('sqlite::memory:'), $savepoints);

        $database = new Database($adapter, new Cache(new None()));
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

    private static function chain(Database $database, RelationType $type, int $depth, bool $twoWay = true): void
    {
        for ($level = 0; $level <= $depth; $level++) {
            $database->createCollection(new Collection(
                id: 'level'.$level,
                attributes: [Attribute::string(key: 'name', size: 64), Attribute::integer(key: 'score')],
                permissions: self::permissions(),
                documentSecurity: true,
            ));
        }

        for ($level = 0; $level < $depth; $level++) {
            $database->createRelationship(new Relationship(
                collection: 'level'.$level,
                relatedCollection: 'level'.($level + 1),
                type: $type,
                twoWay: $twoWay,
                key: 'next',
                twoWayKey: 'prev',
                onDelete: ForeignKeyAction::Cascade,
            ));
        }
    }

    private static function tree(RelationType $type, string $id, int $level, int $depth): Document
    {
        $node = [
            '$id' => $id,
            'name' => 'node '.$id,
            'score' => $level,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
        ];

        if ($level < $depth) {
            $node['next'] = match ($type) {
                RelationType::ManyToOne, RelationType::OneToOne => self::tree($type, $id.'_0', $level + 1, $depth),
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
                $stored[$collection] = \array_map($this->export(...), $rows);
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
