<?php

namespace Tests\Unit\Hook;

use Closure;
use DomainException;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\CursorDirection;

final class QueriesTest extends TestCase
{
    private const string PUBLIC_THEMES = 'public-themes';

    /**
     * @return iterable<string, array{Closure(Database, array<Query>): mixed, mixed}>
     */
    public static function joinedReads(): iterable
    {
        yield 'find' => [static fn (Database $database, array $joins): mixed => self::ids($database->find('tickets', [...$joins, Query::orderAsc('$id')])), ['k1', 'k2', 'k3']];
        yield 'count' => [static fn (Database $database, array $joins): mixed => $database->count('tickets', $joins), 3];
        yield 'sum' => [static fn (Database $database, array $joins): mixed => $database->sum('tickets', 'amount', $joins), 111];
    }

    /**
     * @return iterable<string, array{Closure(Database, string, array<Query>): mixed}>
     */
    public static function reads(): iterable
    {
        yield 'getDocument' => [static fn (Database $database, string $collection, array $queries): mixed => $database->getDocument($collection, 'k1', $queries)];
        yield 'find' => [static fn (Database $database, string $collection, array $queries): mixed => $database->find($collection, $queries)];
        yield 'findOne' => [static fn (Database $database, string $collection, array $queries): mixed => $database->findOne($collection, $queries)];
        yield 'count' => [static fn (Database $database, string $collection, array $queries): mixed => $database->count($collection, $queries)];
        yield 'sum' => [static fn (Database $database, string $collection, array $queries): mixed => $database->sum($collection, 'amount', $queries)];
        yield 'aggregate' => [static fn (Database $database, string $collection, array $queries): mixed => $database->aggregate($collection, [...$queries, Query::count('*', 'total')])];
        yield 'cursor' => [static fn (Database $database, string $collection, array $queries): mixed => \iterator_to_array($database->cursor($collection, $queries), false)];
        yield 'resolveCursor' => [static fn (Database $database, string $collection, array $queries): mixed => $database->resolveCursor($collection, 'k1', CursorDirection::After, $queries)];
    }

    /**
     * The reads that authorize the collections they join; resolveCursor() reads with authorization skipped.
     *
     * @return iterable<string, array{Closure(Database, string, array<Query>): mixed}>
     */
    public static function authorizingReads(): iterable
    {
        foreach (self::reads() as $name => $read) {
            if ($name !== 'resolveCursor') {
                yield $name => $read;
            }
        }
    }

    /**
     * Each read, the event and collection the hook is given on each call it makes, the query the hook adds, and the
     * read's result with that query added.
     *
     * @return iterable<string, array{Closure(Database): mixed, list<array{Event, string}>, Query, mixed}>
     */
    public static function rewrittenReads(): iterable
    {
        $open = Query::equal('status', ['open']);
        $tickets = 'tickets';

        yield 'getDocument' => [
            static fn (Database $database): mixed => \array_intersect_key($database->getDocument($tickets, 'k2')->getArrayCopy(), ['name' => true, 'amount' => true]),
            [[Event::DocumentRead, $tickets]],
            Query::select(['name']),
            ['name' => 'second'],
        ];
        yield 'find' => [
            static fn (Database $database): mixed => self::ids($database->find($tickets, [Query::orderAsc('$id')])),
            [[Event::DocumentFind, $tickets]],
            $open,
            ['k1', 'k3'],
        ];
        yield 'findOne' => [
            static fn (Database $database): mixed => $database->findOne($tickets, [Query::orderDesc('amount')])->getId(),
            [[Event::DocumentFind, $tickets]],
            $open,
            'k3',
        ];
        yield 'count' => [
            static fn (Database $database): mixed => $database->count($tickets),
            [[Event::DocumentCount, $tickets]],
            $open,
            2,
        ];
        yield 'sum' => [
            static fn (Database $database): mixed => $database->sum($tickets, 'amount'),
            [[Event::DocumentSum, $tickets]],
            $open,
            101,
        ];
        yield 'aggregate' => [
            static fn (Database $database): mixed => \array_map(static fn (array $row): array => \array_map(\intval(...), $row), $database->aggregate($tickets, [Query::count('*', 'total')])),
            [[Event::DocumentAggregate, $tickets]],
            $open,
            [['total' => 2]],
        ];
        yield 'cursor' => [
            static fn (Database $database): mixed => self::ids(\iterator_to_array($database->cursor($tickets, [Query::orderAsc('$id')]), false)),
            [[Event::DocumentFind, $tickets], [Event::DocumentFind, $tickets]],
            $open,
            ['k1', 'k3'],
        ];
        yield 'resolveCursor' => [
            static fn (Database $database): mixed => (int) $database->resolveCursor($tickets, 'k2', CursorDirection::After)->getAttribute('amount'),
            [[Event::DocumentFind, $tickets]],
            Query::orderDesc('amount'),
            10,
        ];
    }

    public function testAddingAQueriesHookReturnsTheDatabase(): void
    {
        $database = HookFixture::memory();
        $recorder = new QueriesRecorder();

        $this->assertSame($database, $database->addHook($recorder));

        $database->find(HookFixture::COLLECTION);
        $this->assertSame([[Event::DocumentFind, HookFixture::COLLECTION]], $recorder->calls);
    }

    /**
     * @param  Closure(Database, array<Query>): mixed  $read
     */
    #[DataProvider('joinedReads')]
    public function testAHookMappingAJoinNameLetsTheReadJoin(Closure $read, mixed $expected): void
    {
        $database = $this->joined();

        try {
            $read($database, [$this->publicThemesJoin()]);
            $this->fail('A join of an unknown collection was read');
        } catch (QueryException $error) {
            $this->assertSame("Joined collection 'public-themes' not found", $error->getMessage());
        }

        $database->addHook(new QueriesRecorder(self::mapping(self::PUBLIC_THEMES, 'themes')));

        $this->assertSame($expected, $read($database, [$this->publicThemesJoin()]));
    }

    /**
     * @param  Closure(Database, string, array<Query>): mixed  $read
     */
    #[DataProvider('authorizingReads')]
    public function testTheReadAuthorizesTheCollectionTheHookJoins(Closure $read): void
    {
        $database = $this->joined();
        $database->addHook(new QueriesRecorder(self::mapping(self::PUBLIC_THEMES, 'closed')));

        try {
            $read($database, 'tickets', [$this->publicThemesJoin()]);
            $this->fail('A join of an unreadable collection was read');
        } catch (AuthorizationException $error) {
            $this->assertSame("Unauthorized access to joined collection 'closed'", $error->getMessage());
        }
    }

    /**
     * @param  Closure(Database, string, array<Query>): mixed  $read
     */
    #[DataProvider('reads')]
    public function testARefusalReachesTheCallerBeforeTheReadChecksAnything(Closure $read): void
    {
        $database = $this->joined();
        $refusal = new DomainException('refused');
        $database->addHook(new QueriesRecorder(static fn (array $queries): array => throw $refusal));

        try {
            $read($database, 'sealed', [Query::join('unknown', 'u', [Query::on('theme', '$id')])]);
            $this->fail('A refused read was read');
        } catch (DomainException $error) {
            $this->assertSame($refusal, $error);
        }
    }

    /**
     * @param  Closure(Database): mixed  $read
     * @param  list<array{Event, string}>  $calls
     */
    #[DataProvider('rewrittenReads')]
    public function testEveryReadUsesTheQueriesTheHookReturns(Closure $read, array $calls, Query $added, mixed $expected): void
    {
        $database = $this->joined();
        $recorder = new QueriesRecorder(QueriesRecorder::adding($added));
        $database->addHook($recorder);

        $this->assertSame($expected, $read($database));
        $this->assertSame($calls, $recorder->calls);
    }

    public function testFindOneRunsTheHookOnce(): void
    {
        $database = $this->posts();
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::equal('title', ['second'])));
        $database->addHook($recorder);

        $this->assertSame('second', $database->findOne(HookFixture::COLLECTION)->getId());
        $this->assertSame([[Event::DocumentFind, HookFixture::COLLECTION]], $recorder->calls);
    }

    public function testACursorPagesPastItsFirstBatchOverAJoinOnlyTheHookResolves(): void
    {
        $database = $this->joined();
        $database->addHook(new QueriesRecorder(self::mapping(self::PUBLIC_THEMES, 'themes')));

        $documents = $database->cursor('tickets', [$this->publicThemesJoin(), Query::orderAsc('$id')], batchSize: 1);

        $this->assertSame(['k1', 'k2', 'k3'], self::ids(\iterator_to_array($documents, false)));
    }

    public function testHooksRunInRegistrationOrderEachGivenThePreviousOutput(): void
    {
        $database = $this->posts();
        $order = [];
        $output = [];
        $given = [];
        $adding = QueriesRecorder::adding(Query::equal('title', ['first']));
        $first = new QueriesRecorder(static function (array $queries) use ($adding, &$order, &$output): array {
            $order[] = 'first';
            $output = $adding($queries);

            return $output;
        });
        $second = new QueriesRecorder(static function (array $queries) use (&$order, &$given): array {
            $order[] = 'second';
            $given = $queries;

            return $queries;
        });
        $database->addHook($first)->addHook($second);

        $this->assertSame(['first'], self::ids($database->find(HookFixture::COLLECTION)));
        $this->assertSame(['first', 'second'], $order);
        $this->assertNotSame([], $given);
        $this->assertSame($output, $given);
    }

    public function testTheHookIsNeverGivenAReadOfTheMetadataCollection(): void
    {
        $database = $this->posts();
        $recorder = new QueriesRecorder();
        $database->addHook($recorder);

        $database->getCollection(HookFixture::COLLECTION);
        $database->listCollections();
        $database->getDocument(Database::METADATA, HookFixture::COLLECTION);
        $database->find(Database::METADATA);
        $database->count(Database::METADATA);
        $database->getDocument(HookFixture::COLLECTION, 'first');

        $this->assertSame([[Event::DocumentRead, HookFixture::COLLECTION]], $recorder->calls);
    }

    public function testBulkWritesWithFiltersDoNotRunTheHook(): void
    {
        $database = $this->posts();
        $recorder = new QueriesRecorder();
        $database->addHook($recorder);

        $this->assertSame(1, $database->updateDocuments(HookFixture::COLLECTION, new Document(['views' => 9]), [Query::equal('title', ['first'])]));
        $this->assertSame(1, $database->deleteDocuments(HookFixture::COLLECTION, [Query::equal('title', ['second'])]));

        $this->assertSame([], $recorder->calls);
    }

    public function testAHookReturningSomethingOtherThanAQueryIsRefused(): void
    {
        $database = $this->posts();
        $database->addHook(new QueriesRecorder(static fn (array $queries): array => [...$queries, 'title']));

        try {
            $database->find(HookFixture::COLLECTION);
            $this->fail('A query that is not a Query was read');
        } catch (QueryException $error) {
            $this->assertSame('Invalid query type: "string". Expected instances of "'.Query::class.'"', $error->getMessage());
        }
    }

    public function testTheHookRunsWhileEventsAreSilenced(): void
    {
        $database = $this->posts();
        $recorder = new QueriesRecorder(QueriesRecorder::adding(Query::equal('title', ['second'])));
        $database->addHook($recorder);

        $documents = $database->silent(static fn (): array => $database->find(HookFixture::COLLECTION));

        $this->assertSame(['second'], self::ids($documents));
        $this->assertSame([[Event::DocumentFind, HookFixture::COLLECTION]], $recorder->calls);
    }

    public function testWritesThatReadOnlyThroughTheirOwnDocumentReadDoNotRunTheHook(): void
    {
        $database = $this->posts();
        $recorder = new QueriesRecorder();
        $database->addHook($recorder);

        $increased = $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', 5);
        $deleted = $database->deleteDocument(HookFixture::COLLECTION, 'second');

        $this->assertSame([], $recorder->calls);
        $this->assertSame(6, $increased->getAttribute('views'));
        $this->assertTrue($deleted);
    }

    public function testACursorWithoutHooksOverAMissingCollectionThrowsOnlyOnceIterated(): void
    {
        $database = HookFixture::memory();

        try {
            $documents = $database->cursor('missing');
        } catch (NotFoundException) {
            $this->fail('cursor() resolved its collection before it was iterated');
        }

        try {
            \iterator_to_array($documents);
            $this->fail('A cursor over a missing collection was iterated');
        } catch (NotFoundException $error) {
            $this->assertSame('Collection not found', $error->getMessage());
        }
    }

    /**
     * @return Closure(array<Query>): array<Query>
     */
    private static function mapping(string $from, string $to): Closure
    {
        return static fn (array $queries): array => \array_map(
            static fn (Query $query): Query => $query->getMethod()->isJoin() && $query->getAttribute() === $from
                ? (clone $query)->setAttribute($to)
                : $query,
            $queries,
        );
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private static function ids(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }

    private function publicThemesJoin(): Query
    {
        return Query::join(self::PUBLIC_THEMES, 'th', [Query::on('theme', '$id')]);
    }

    private function posts(): Database
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second']);

        return $database;
    }

    private function joined(): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $connection = new PDO('sqlite::memory:', null, null, [PDO::ATTR_PERSISTENT => false, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC, PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_EMULATE_PREPARES => true, PDO::ATTR_STRINGIFY_FETCHES => true]);
        $database = new Database(new SQLite($connection), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('queries_hook')
            ->setNamespace('queries_hook_'.\uniqid());
        $database->addHook(new Permissions());
        $database->create();

        $readable = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: 'themes',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: $readable,
            documentSecurity: false,
        ));
        $database->createCollection(Collection::create(
            id: 'closed',
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: [],
            documentSecurity: false,
        ));
        $tickets = [
            Attribute::string(key: 'name', size: 64),
            Attribute::string(key: 'theme', size: 64),
            Attribute::string(key: 'status', size: 16),
            Attribute::integer(key: 'amount'),
        ];
        $database->createCollection(Collection::create(id: 'tickets', attributes: $tickets, permissions: $readable, documentSecurity: false));
        $database->createCollection(Collection::create(id: 'sealed', attributes: $tickets, permissions: [], documentSecurity: false));

        foreach (['t1' => 'first theme', 't2' => 'second theme', 't3' => 'third theme'] as $id => $name) {
            $database->createDocument('themes', new Document(['$id' => $id, 'name' => $name]));
        }

        foreach ([
            ['k1', 'first', 't1', 'open', 1],
            ['k2', 'second', 't2', 'closed', 10],
            ['k3', 'third', 't3', 'open', 100],
            ['k4', 'fourth', 'missing', 'closed', 1000],
        ] as [$id, $name, $theme, $status, $amount]) {
            $database->createDocument('tickets', new Document([
                '$id' => $id,
                'name' => $name,
                'theme' => $theme,
                'status' => $status,
                'amount' => $amount,
            ]));
        }

        return $database;
    }
}
