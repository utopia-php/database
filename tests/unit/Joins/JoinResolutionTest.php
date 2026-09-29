<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\HashAwareMemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Query\Join as JoinValidator;
use Utopia\Query\Method;

/**
 * A join read resolves the collections it joins and reads their attributes as a direct read of
 * each collection reads them.
 */
final class JoinResolutionTest extends TestCase
{
    private const string LATER_THAN_EIGHT_UTC = '2024-01-01T10:00:00.000+02:00';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = $this->database(new Cache(new None()));
    }

    /**
     * @return iterable<string, array{Query, Query}>
     */
    public static function joinedFilters(): iterable
    {
        yield 'containsAny on a joined array' => [Query::containsAny('th.tags', ['a']), Query::containsAny('tags', ['a'])];
        yield 'contains on a joined array' => [Query::contains('th.tags', ['a']), Query::contains('tags', ['a'])];
        yield 'containsAll on a joined array' => [Query::containsAll('th.tags', ['a', 'b']), Query::containsAll('tags', ['a', 'b'])];
        yield 'notContains on a joined array' => [Query::notContains('th.tags', ['a']), Query::notContains('tags', ['a'])];
        yield 'greaterThan on a joined datetime with an offset' => [Query::greaterThan('th.when', self::LATER_THAN_EIGHT_UTC), Query::greaterThan('when', self::LATER_THAN_EIGHT_UTC)];
        yield 'equal on a joined datetime in UTC' => [Query::equal('th.when', ['2024-01-01T09:00:00.000+00:00']), Query::equal('when', ['2024-01-01T09:00:00.000+00:00'])];
        yield 'equal on a joined datetime with an offset' => [Query::equal('th.when', ['2024-01-01T11:00:00.000+02:00']), Query::equal('when', ['2024-01-01T11:00:00.000+02:00'])];
        yield 'a joined filter inside or()' => [
            Query::or([Query::containsAny('th.tags', ['c']), Query::lessThan('th.when', '2024-01-01T09:30:00.000+02:00')]),
            Query::or([Query::containsAny('tags', ['c']), Query::lessThan('when', '2024-01-01T09:30:00.000+02:00')]),
        ];
    }

    /**
     * The joined filters a join's ON list holds: every one but containsAll.
     *
     * @return iterable<string, array{Query, Query}>
     */
    public static function joinConditionFilters(): iterable
    {
        foreach (self::joinedFilters() as $name => $filters) {
            if ($filters[0]->getMethod() !== Method::ContainsAll) {
                yield $name => $filters;
            }
        }
    }

    #[DataProvider('joinedFilters')]
    public function testAJoinedFilterMatchesWhatTheSameFilterMatchesOnTheJoinedCollection(Query $joined, Query $direct): void
    {
        $expected = $this->ticketsOfThemes($this->database->find('themes', [$direct]));
        $this->assertNotSame([], $expected, 'the fixture has to match some rows');
        $this->assertNotSame(['k1', 'k2', 'k3', 'k4'], $expected, 'the fixture has to leave some rows out');

        $join = Query::join('themes', 'theme', '$id', '=', 'th');

        $this->assertSame($expected, $this->ids($this->database->find('tickets', [$join, $joined])), 'find()');
        $this->assertSame(\count($expected), $this->database->count('tickets', [$join, $joined]), 'count()');
        $this->assertSame($this->amounts($expected), $this->database->sum('tickets', 'amount', [$join, $joined]), 'sum()');
    }

    #[DataProvider('joinConditionFilters')]
    public function testAJoinedFilterInAJoinConditionMatchesWhatItMatchesOnTheJoinedCollection(Query $joined, Query $direct): void
    {
        $expected = $this->ticketsOfThemes($this->database->find('themes', [$direct]));
        $join = Query::join('themes', 'th', [Query::on('theme', '$id'), $joined]);

        $this->assertSame($expected, $this->ids($this->database->find('tickets', [$join])), 'find()');
        $this->assertSame(\count($expected), $this->database->count('tickets', [$join]), 'count()');
        $this->assertSame($this->amounts($expected), $this->database->sum('tickets', 'amount', [$join]), 'sum()');

        foreach (['k1', 'k2', 'k3', 'k4'] as $ticket) {
            $this->assertSame(
                \in_array($ticket, $expected, true),
                ! $this->database->getDocument('tickets', $ticket, [$join])->isEmpty(),
                'getDocument('.$ticket.')',
            );
        }
    }

    public function testAJoinedCreatedAtWithAnOffsetMatchesWhatItMatchesOnTheJoinedCollection(): void
    {
        $created = $this->database->getDocument('themes', 't2')->getCreatedAt();
        $this->assertIsString($created);
        $sameInstantElsewhere = (new \DateTimeImmutable($created))
            ->setTimezone(new \DateTimeZone('+14:00'))
            ->format('Y-m-d\TH:i:s.vP');

        $expected = $this->ticketsOfThemes($this->database->find('themes', [Query::equal('$createdAt', [$sameInstantElsewhere])]));
        $this->assertContains('k2', $expected);

        $this->assertSame($expected, $this->ids($this->database->find('tickets', [
            Query::join('themes', 'theme', '$id', '=', 'th'),
            Query::equal('th.$createdAt', [$sameInstantElsewhere]),
        ])));
    }

    public function testAHavingConditionOnAGroupedDatetimeIsComparedAsAFilterComparesIt(): void
    {
        $expected = [];
        foreach ($this->database->find('themes', [Query::greaterThan('when', self::LATER_THAN_EIGHT_UTC)]) as $theme) {
            $when = $theme->getAttribute('when');
            $this->assertIsString($when);
            $expected[] = $when;
        }
        \sort($expected);
        $this->assertSame(['2024-01-01T09:00:00.000+00:00', '2024-01-01T11:00:00.000+00:00'], $expected);

        $joined = $this->database->find('tickets', [
            Query::join('themes', 'theme', '$id', '=', 'th'),
            Query::count('*', 'total'),
            Query::groupBy(['th.when']),
            Query::having([Query::greaterThan('th.when', self::LATER_THAN_EIGHT_UTC)]),
        ]);
        $this->assertCount(2, $joined, 'having on a joined grouped datetime');

        $main = $this->database->find('tickets', [
            Query::count('*', 'total'),
            Query::groupBy(['when']),
            Query::having([Query::greaterThan('when', self::LATER_THAN_EIGHT_UTC)]),
        ]);
        $this->assertCount(2, $main, 'having on a grouped datetime of the main collection');
    }

    public function testAMaximumOfADatetimeIsComparedInHavingAsTheDatetimeIs(): void
    {
        $rows = $this->database->find('tickets', [
            Query::join('themes', 'theme', '$id', '=', 'th'),
            Query::max('th.when', 'latest'),
            Query::groupBy(['name']),
            Query::having([Query::greaterThan('latest', self::LATER_THAN_EIGHT_UTC)]),
        ]);

        $names = \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows);
        \sort($names);
        $this->assertSame(['first', 'third'], $names);
    }

    public function testAJoinReadResolvesEachJoinedCollectionOnce(): void
    {
        $cache = new class () extends HashAwareMemoryCache {
            /**
             * @var list<string>
             */
            public array $loads = [];

            public function load(string $key, int $ttl, string $hash = ''): mixed
            {
                $this->loads[] = $key;

                return parent::load($key, $ttl, $hash);
            }
        };
        $this->database = $this->database(new Cache($cache));

        $join = Query::join('themes', 'theme', '$id', '=', 'th');
        $selfJoin = Query::join('themes', 'th.$id', '$id', '=', 'tx');
        $reads = [
            'find()' => fn (): mixed => $this->database->find('tickets', [$join, Query::containsAny('th.tags', ['a'])]),
            'find() of an aggregate' => fn (): mixed => $this->database->find('tickets', [$join, Query::sum('th.score', 'total')]),
            'find() of a self-join' => fn (): mixed => $this->database->find('tickets', [$join, $selfJoin]),
            'count()' => fn (): mixed => $this->database->count('tickets', [$join, $selfJoin]),
            'sum() of a joined attribute' => fn (): mixed => $this->database->sum('tickets', 'th.score', [$join, $selfJoin]),
            'getDocument()' => fn (): mixed => $this->database->getDocument('tickets', 'k1', [$join, $selfJoin]),
        ];

        $lookups = [];
        foreach ($reads as $name => $read) {
            $read();
            $cache->loads = [];
            $read();

            $lookups[$name] = \count(\array_filter(
                $cache->loads,
                static fn (string $key): bool => \str_ends_with($key, ':'.Database::METADATA.':themes'),
            ));
        }

        $this->assertSame(\array_fill_keys(\array_keys($reads), 1), $lookups);
    }

    public function testMoreJoinsThanTheCapAreRefusedWithoutValidation(): void
    {
        $joins = static fn (int $count): array => \array_map(
            static fn (int $index): Query => Query::join('themes', 'theme', '$id', '=', 'th'.$index),
            \range(1, $count),
        );

        $reads = [
            'find()' => fn (array $queries): mixed => $this->database->find('tickets', $queries),
            'count()' => fn (array $queries): mixed => $this->database->count('tickets', $queries),
            'sum()' => fn (array $queries): mixed => $this->database->sum('tickets', 'amount', $queries),
            'getDocument()' => fn (array $queries): mixed => $this->database->getDocument('tickets', 'k1', $queries),
        ];

        foreach ($reads as $name => $read) {
            $this->database->skipValidation(fn (): mixed => $read($joins(JoinValidator::MAX_PER_QUERY)));

            try {
                $this->database->skipValidation(fn (): mixed => $read($joins(JoinValidator::MAX_PER_QUERY + 1)));
                $this->fail($name.': '.(JoinValidator::MAX_PER_QUERY + 1).' joins ran without validation');
            } catch (QueryException $error) {
                $this->assertSame('Too many joins: at most '.JoinValidator::MAX_PER_QUERY.' are allowed', $error->getMessage(), $name);
            }
        }
    }

    public function testAnUnmatchedOuterRowWithoutASelectedIdIsDropped(): void
    {
        $join = Query::leftJoin('themes', 'theme', '$id', '=', 'th');
        $select = Query::select(['name', 'th.tags', 'th.when']);

        $rows = [];
        foreach ($this->database->find('tickets', [$join, $select]) as $row) {
            $rows[$row->getId()] = $row;
        }
        $rows['k4 read by id'] = $this->database->getDocument('tickets', 'k4', [$join, $select]);
        $rows['k2 read by id'] = $this->database->getDocument('tickets', 'k2', [$join, $select]);

        foreach ($rows as $name => $row) {
            $this->assertFalse($row->offsetExists('th.$id'), $name.': the joined $id was not selected');
        }
        foreach (['k4', 'k4 read by id'] as $name) {
            $this->assertNull($rows[$name]->getAttribute('th.tags'), $name);
            $this->assertNull($rows[$name]->getAttribute('th.when'), $name);
        }
        foreach (['k2', 'k2 read by id'] as $name) {
            $this->assertSame(['a', 'b'], $rows[$name]->getAttribute('th.tags'), $name);
            $this->assertSame('2024-01-01T07:00:00.000+00:00', $rows[$name]->getAttribute('th.when'), $name);
        }

        $selected = $this->database->find('tickets', [$join, Query::select(['name', 'th.$id', 'th.tags']), Query::equal('$id', ['k2', 'k4'])]);
        $this->assertSame(
            [['k2', 't2', ['a', 'b']], ['k4', null, null]],
            \array_map(static fn (Document $row): array => [$row->getId(), $row->getAttribute('th.$id'), $row->getAttribute('th.tags')], $selected),
        );
    }

    public function testADistinctOuterJoinReadSelectsNoJoinedIdOfItsOwn(): void
    {
        $rows = $this->database->find('tickets', [
            Query::leftJoin('themes', 'theme', '$id', '=', 'th'),
            Query::select(['th.score']),
            Query::distinct(),
        ]);

        $scores = \array_map(static fn (Document $row): mixed => $row->getAttribute('th.score'), $rows);
        \sort($scores);
        $this->assertSame([null, 5], $scores);
    }

    public function testAJoinedSelectLeavesUnselectedAttributesOut(): void
    {
        $join = Query::join('themes', 'theme', '$id', '=', 'th');
        $select = Query::select(['name', 'th.name']);

        $rows = $this->database->find('tickets', [$join, $select, Query::equal('$id', ['k1'])]);
        $rows[] = $this->database->getDocument('tickets', 'k1', [$join, $select]);

        foreach ($rows as $row) {
            $this->assertSame('first', $row->getAttribute('name'));
            $this->assertSame('banana theme', $row->getAttribute('th.name'));
            foreach (['tags', 'amount', 'when', 'theme'] as $unselected) {
                $this->assertFalse($row->offsetExists($unselected), $unselected.' was not selected');
            }
        }
    }

    /**
     * @param  array<Document>  $themes
     * @return list<string>
     */
    private function ticketsOfThemes(array $themes): array
    {
        $themeIds = \array_map(static fn (Document $theme): string => $theme->getId(), $themes);

        return $this->ids($this->database->find('tickets', [Query::equal('theme', $themeIds === [] ? ['none'] : $themeIds)]));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $documents);
        \sort($ids);

        return $ids;
    }

    /**
     * @param  list<string>  $tickets
     */
    private function amounts(array $tickets): int
    {
        $amounts = ['k1' => 1, 'k2' => 10, 'k3' => 100, 'k4' => 1000];

        return \array_sum(\array_map(static fn (string $ticket): int => $amounts[$ticket], $tickets));
    }

    private function database(Cache $cache): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $connection = new PDO('sqlite::memory:', null, null, [PDO::ATTR_PERSISTENT => false] + SQLite::getPDOAttributes());
        $database = new Database(new SQLite($connection), $cache);
        $database
            ->setAuthorization($authorization)
            ->setDatabase('join_resolution')
            ->setNamespace('join_resolution_'.\uniqid());
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: 'themes',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::string(key: 'tags', size: 32, array: true),
                Attribute::datetime(key: 'when'),
                Attribute::integer(key: 'score'),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: 'tickets',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::string(key: 'theme', size: 64),
                Attribute::string(key: 'tags', size: 32, array: true),
                Attribute::integer(key: 'amount'),
                Attribute::datetime(key: 'when'),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));

        foreach ([
            ['t1', 'banana theme', ['banana'], '2024-01-01T09:00:00.000+00:00'],
            ['t2', 'ab theme', ['a', 'b'], '2024-01-01T07:00:00.000+00:00'],
            ['t3', 'c theme', ['b', 'c'], '2024-01-01T11:00:00.000+00:00'],
        ] as [$id, $name, $tags, $when]) {
            $database->createDocument('themes', new Document(['$id' => $id, 'name' => $name, 'tags' => $tags, 'when' => $when, 'score' => 5]));
        }

        foreach ([
            ['k1', 'first', 't1', 1, '2024-01-01T09:00:00.000+00:00'],
            ['k2', 'second', 't2', 10, '2024-01-01T07:00:00.000+00:00'],
            ['k3', 'third', 't3', 100, '2024-01-01T11:00:00.000+00:00'],
            ['k4', 'fourth', 'missing', 1000, '2024-01-01T06:00:00.000+00:00'],
        ] as [$id, $name, $theme, $amount, $when]) {
            $database->createDocument('tickets', new Document([
                '$id' => $id,
                'name' => $name,
                'theme' => $theme,
                'tags' => ['x', 'y'],
                'amount' => $amount,
                'when' => $when,
            ]));
        }

        return $database;
    }
}
