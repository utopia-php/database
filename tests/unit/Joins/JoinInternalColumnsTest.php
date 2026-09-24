<?php

namespace Tests\Unit\Joins;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
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
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * Every join and aggregate shape the validators accept runs, and every shape the engine cannot run is
 * an invalid query: an internal attribute under a join alias groups the joined rows, `$collection`
 * and, without shared tables, `$tenant` are never aggregated or grouped, a join condition names
 * columns its tables have, sum() adds up only what a sum aggregate accepts, and an encrypted joined
 * attribute cannot be filtered.
 */
final class JoinInternalColumnsTest extends TestCase
{
    private const string TENANT = '1';

    private Database $database;

    protected function setUp(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')));
    }

    /**
     * Each join of the customers to their notes, with the number of rows it returns: c3 has no note
     * and n4 no customer.
     *
     * @return iterable<string, array{Method, bool, int}>
     */
    public static function joins(): iterable
    {
        yield 'inner join' => [Method::Join, false, 3];
        yield 'left join' => [Method::LeftJoin, false, 4];
        yield 'right join' => [Method::RightJoin, false, 4];
        yield 'emulated full outer join' => [Method::FullOuterJoin, false, 5];
        yield 'native full outer join' => [Method::FullOuterJoin, true, 5];
    }

    /**
     * @return iterable<string, array{Method, bool, int, string}>
     */
    public static function internalAttributesUnderEveryJoin(): iterable
    {
        foreach (self::joins() as $label => [$join, $native, $rows]) {
            foreach ([Document::ID, Document::SEQUENCE, Document::CREATED_AT, Document::UPDATED_AT, Document::PERMISSIONS] as $attribute) {
                yield $attribute.' over a '.$label => [$join, $native, $rows, $attribute];
            }
        }
    }

    #[DataProvider('internalAttributesUnderEveryJoin')]
    public function testInternalAttributeUnderAnAliasGroupsTheJoinedRows(Method $join, bool $native, int $rows, string $attribute): void
    {
        if ($native) {
            $this->useDatabase(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $groups = $this->database->find('customers', [
            $this->join($join),
            Query::count('*', 'rows'),
            Query::groupBy(['note.'.$attribute]),
        ]);

        $total = 0;
        foreach ($groups as $group) {
            $this->assertArrayHasKey(Storage::column($attribute), $group->getArrayCopy(), 'a group comes back under its column, as on the main collection');
            $count = $group->getAttribute('rows');
            $this->assertIsInt($count);
            $total += $count;
        }
        $this->assertSame($rows, $total);
    }

    /**
     * @return iterable<string, array{Method, bool, list<string|null>}>
     */
    public static function joinedIdGroups(): iterable
    {
        yield 'inner join' => [Method::Join, false, ['n1', 'n2', 'n3']];
        yield 'left join' => [Method::LeftJoin, false, [null, 'n1', 'n2', 'n3']];
        yield 'right join' => [Method::RightJoin, false, ['n1', 'n2', 'n3', 'n4']];
        yield 'emulated full outer join' => [Method::FullOuterJoin, false, [null, 'n1', 'n2', 'n3', 'n4']];
        yield 'native full outer join' => [Method::FullOuterJoin, true, [null, 'n1', 'n2', 'n3', 'n4']];
    }

    /**
     * @param  list<string|null>  $expected
     */
    #[DataProvider('joinedIdGroups')]
    public function testJoinedIdGroupsOneRowPerJoinedDocument(Method $join, bool $native, array $expected): void
    {
        if ($native) {
            $this->useDatabase(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $groups = $this->database->find('customers', [
            $this->join($join),
            Query::count('*', 'rows'),
            Query::groupBy(['note.$id']),
            Query::orderAsc('note.$id'),
        ]);

        $this->assertSame($expected, \array_map(static fn (Document $group): mixed => $group->getAttribute(Storage::UID), $groups));
    }

    public function testCollectionIsNeitherAggregatedNorGrouped(): void
    {
        foreach ([
            'count' => [Query::count('$collection', 'total')],
            'countDistinct' => [Query::countDistinct('$collection', 'total')],
            'groupBy' => [Query::count('*', 'rows'), Query::groupBy(['$collection'])],
            'groupBy over a join' => [$this->join(Method::Join), Query::count('*', 'rows'), Query::groupBy(['$collection'])],
        ] as $label => $queries) {
            $this->assertInvalidQuery('Invalid query: Attribute not found in schema: $collection', fn (): mixed => $this->database->find('customers', $queries), $label);
        }

        $customers = $this->database->find('customers', [Query::select(['name', '$collection']), Query::orderAsc('name')]);
        $this->assertSame(['customers', 'customers', 'customers'], \array_map(static fn (Document $customer): string => $customer->getCollection(), $customers), 'a read still derives $collection');
    }

    public function testTenantIsRejectedWithoutSharedTables(): void
    {
        $note = $this->join(Method::Join);

        foreach ([
            'count' => [[Query::count('$tenant', 'total')], '$tenant'],
            'groupBy' => [[Query::count('*', 'rows'), Query::groupBy(['$tenant'])], '$tenant'],
            'select' => [[Query::select(['name', '$tenant'])], '$tenant'],
            'joined count' => [[$note, Query::count('note.$tenant', 'total')], 'note.$tenant'],
            'joined groupBy' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.$tenant'])], 'note.$tenant'],
            'joined select' => [[$note, Query::select(['name', 'note.$tenant'])], 'note.$tenant'],
        ] as $label => [$queries, $attribute]) {
            $this->assertInvalidQuery('Invalid query: Attribute not found in schema: '.$attribute, fn (): mixed => $this->database->find('customers', $queries), $label);
        }

        $this->assertInvalidQuery(
            'Invalid query: Attribute not found in schema: note.$tenant',
            fn (): mixed => $this->database->getDocument('customers', 'c1', [
                Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')]),
                Query::select(['name', 'note.$tenant']),
            ]),
        );
    }

    public function testTenantIsReadUnderSharedTables(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')), sharedTables: true);
        $note = $this->join(Method::Join);

        $this->assertSame([['total' => 3]], $this->rows($this->database->find('customers', [Query::count('$tenant', 'total')])));
        $this->assertSame([['rows' => 3, Storage::TENANT => 1]], $this->rows($this->database->find('customers', [Query::count('*', 'rows'), Query::groupBy(['$tenant'])])));
        $this->assertSame([['total' => 3]], $this->rows($this->database->find('customers', [$note, Query::count('note.$tenant', 'total')])));
        $this->assertSame([['rows' => 3, Storage::TENANT => 1]], $this->rows($this->database->find('customers', [$note, Query::count('*', 'rows'), Query::groupBy(['note.$tenant'])])));

        $customers = $this->database->find('customers', [$note, Query::select(['name', 'note.$tenant']), Query::orderAsc('note.$id')]);
        $this->assertSame([self::TENANT, self::TENANT, self::TENANT], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('note.$tenant'), $customers));
    }

    /**
     * @return iterable<string, array{list<Query>, string}>
     */
    public static function joinConditionsNamingNoColumn(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');
        $notFound = 'Invalid query: Attribute not found in schema: ';

        yield 'an unknown right column' => [[Query::join('notes', '$id', 'nothing', '=', 'note')], $notFound.'nothing'];
        yield 'an unknown left column' => [[Query::join('notes', 'nothing', 'customerId', '=', 'note')], $notFound.'nothing'];
        yield 'an unknown right column of an on condition' => [[Query::leftJoin('notes', 'note', [Query::on('$id', 'nothing')])], $notFound.'nothing'];
        yield 'an unknown left column of an on condition' => [[Query::leftJoin('notes', 'note', [Query::on('nothing', 'customerId')])], $notFound.'nothing'];
        yield 'an unknown right column under the join alias' => [[Query::leftJoin('notes', 'note', [Query::on('$id', 'note.nothing')])], $notFound.'note.nothing'];
        yield 'an unknown column of an earlier join' => [[$note, Query::join('replies', 'note.nothing', 'noteId', '=', 'reply')], $notFound.'note.nothing'];
        yield 'a main attribute under the join alias' => [[Query::rightJoin('notes', '$id', 'note.name', '=', 'note')], $notFound.'note.name'];
        yield 'a join declared after it' => [
            [Query::join('replies', 'note.$id', 'noteId', '=', 'reply'), $note],
            'Invalid query: The left column of a join condition must belong to the main collection or to a join declared before it: note.$id',
        ];
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('joinConditionsNamingNoColumn')]
    public function testJoinConditionNamingNoColumnIsAnInvalidQuery(array $joins, string $message): void
    {
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->find('customers', $joins), 'find()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->count('customers', $joins), 'count()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->sum('customers', 'score', $joins), 'sum()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->getDocument('customers', 'c1', $joins), 'getDocument()');
    }

    public function testJoinConditionOverColumnsTheTablesHaveRuns(): void
    {
        $note = $this->join(Method::Join);

        $this->assertSame(3, $this->database->count('customers', [Query::leftJoin('notes', 'note', [Query::on('$id', 'note.customerId')]), Query::isNotNull('note.$id')]));
        $this->assertSame(1, $this->database->count('customers', [$note, Query::join('replies', 'note.$id', 'noteId', '=', 'reply')]), 'a chained join reads the join before it');
        $this->assertSame(1, $this->database->count('customers', [Query::crossJoin('replies', 'reply'), Query::join('notes', 'reply.noteId', '$id', '=', 'note'), Query::equal('note.customerId', ['c1']), Query::equal('$id', ['c1'])]), 'a cross join declares its alias for the joins after it');
        $this->assertSame(3, $this->database->count('customers', [Query::join('notes', '$sequence', '$sequence', '<', 'note'), Query::equal('note.$id', ['n4'])]), 'internal columns are compared on both sides');
    }

    public function testJoinConditionOverARelationshipNamesItsColumn(): void
    {
        $this->useRelationships();

        $persons = $this->database->find('persons', [Query::join('libraries', 'library', '$id', '=', 'lib'), Query::select(['name', 'lib.name'])]);
        $this->assertSame(['Central'], \array_map(static fn (Document $person): mixed => $person->getAttribute('lib.name'), $persons), 'the parent side of a one-to-one relationship holds a column');

        $this->assertSame([['rows' => 2]], $this->rows($this->database->find('persons', [Query::join('books', '$id', 'owner', '=', 'book'), Query::count('*', 'rows')])), 'the child side of a one-to-many relationship holds a column');
        $this->assertSame(2, $this->database->count('books', [Query::join('persons', 'owner', '$id', '=', 'person')]));

        $this->assertInvalidQuery(
            'Invalid query: Cannot join on virtual relationship attribute: books',
            fn (): mixed => $this->database->find('persons', [Query::join('books', 'books', '$id', '=', 'book')]),
        );
        $this->assertInvalidQuery(
            'Invalid query: Cannot join on virtual relationship attribute: person',
            fn (): mixed => $this->database->find('persons', [Query::join('libraries', '$id', 'person', '=', 'lib')]),
        );
    }

    /**
     * @return iterable<string, array{string, bool, string}>
     */
    public static function attributesSumCannotAddUp(): iterable
    {
        $numeric = 'Invalid query: Aggregate sum requires a numeric attribute that is not an array: ';
        $notFound = 'Invalid query: Attribute not found in schema: ';

        yield 'an unknown attribute' => ['nothing', false, $notFound.'nothing'];
        yield 'a string' => ['name', false, $numeric.'name'];
        yield 'an array' => ['tags', false, $numeric.'tags'];
        yield 'an internal attribute' => ['$sequence', false, $numeric.'$sequence'];
        yield 'a joined string' => ['note.body', true, $numeric.'note.body'];
        yield 'a joined array' => ['note.tags', true, $numeric.'note.tags'];
        yield 'an unknown joined attribute' => ['note.nothing', true, $notFound.'note.nothing'];
        yield 'an attribute only a join declares, unqualified' => ['body', true, $notFound.'body'];
        yield 'an alias no join declares' => ['other.score', true, $notFound.'other.score'];
    }

    #[DataProvider('attributesSumCannotAddUp')]
    public function testSumRejectsAnAttributeASumAggregateRejects(string $attribute, bool $joined, string $message): void
    {
        $queries = $joined ? [$this->join(Method::Join)] : [];

        $this->assertInvalidQuery($message, fn (): mixed => $this->database->sum('customers', $attribute, $queries));
    }

    public function testSumAddsUpANumericAttributeOfTheMainOrAJoinedCollection(): void
    {
        $note = $this->join(Method::Join);

        $this->assertSame(60, $this->database->sum('customers', 'score'));
        $this->assertSame(40, $this->database->sum('customers', 'score', [$note]), 'a bare name the join declares too stays on the main collection');
        $this->assertSame(6, $this->database->sum('customers', 'note.score', [$note]));
        $this->assertSame(0, $this->database->sum('customers', 'note.score', [$note, Query::equal('note.body', ['none'])]));
    }

    public function testEncryptedJoinedAttributeCannotBeFiltered(): void
    {
        $vault = Query::join('secrets', '$id', 'holderId', '=', 'vault');

        $this->assertInvalidQuery('Invalid query: Cannot query encrypted attribute: secret', fn (): mixed => $this->database->find('secrets', [Query::equal('secret', ['x'])]));
        $this->assertInvalidQuery('Invalid query: Cannot query encrypted attribute: vault.secret', fn (): mixed => $this->database->find('customers', [$vault, Query::equal('vault.secret', ['x'])]));
        $this->assertInvalidQuery('Invalid query: Cannot query encrypted attribute: vault.secret', fn (): mixed => $this->database->count('customers', [$vault, Query::isNull('vault.secret')]));
        $this->assertInvalidQuery('Invalid query: Cannot query encrypted attribute: vault.secret', fn (): mixed => $this->database->find('customers', [$vault, Query::or([Query::equal('name', ['One']), Query::equal('vault.secret', ['x'])])]));

        $this->assertSame([], $this->database->find('customers', [$vault, Query::equal('vault.holderId', ['c1'])]));
    }

    /**
     * What a join could already read stays readable: internal attributes under an alias where they
     * are valid on the main collection, and every declared joined attribute.
     */
    public function testWhatJoinsAlreadyReadStaysReadable(): void
    {
        $note = $this->join(Method::Join);

        $customers = $this->database->find('customers', [
            $note,
            Query::equal('note.$id', ['n1', 'n3']),
            Query::between('note.$createdAt', '1970-01-01', '2099-12-31'),
            Query::between('note.score', 0, 10),
            Query::equal('note.score', [1, 3]),
            Query::select(['name', 'note.$id', 'note.$permissions', 'note.$createdAt', 'note.$sequence', 'note.body']),
            Query::orderAsc('note.score'),
        ]);
        $this->assertSame(['n1', 'n3'], \array_map(static fn (Document $customer): mixed => $customer->getAttribute('note.$id'), $customers));

        $this->assertSame([['notes' => 3, 'total' => 6]], $this->rows($this->database->find('customers', [$note, Query::count('note.$id', 'notes'), Query::sum('note.score', 'total')])));
        $this->assertCount(3, $this->database->find('customers', [$note, Query::count('*', 'rows'), Query::groupBy(['note.body'])]));
    }

    private function join(Method $method): Query
    {
        return new Query($method, 'notes', ['$id', '=', 'customerId', 'note']);
    }

    /**
     * @param  Closure(): mixed  $read
     */
    private function assertInvalidQuery(string $message, Closure $read, string $label = ''): void
    {
        $prefix = $label === '' ? '' : $label.': ';

        try {
            $read();
        } catch (QueryException $error) {
            $this->assertSame($message, $error->getMessage(), $prefix.'the rejection names the shape');

            return;
        }

        $this->fail($prefix.'the shape was accepted: '.$message);
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array<string, mixed>>
     */
    private function rows(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): array => $document->getArrayCopy(), $documents));
    }

    private function useDatabase(SQLite $adapter, bool $sharedTables = false): void
    {
        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase('join_internal_columns')
            ->setNamespace('join_internal_columns_'.\uniqid())
            ->setAuthorization(new Authorization());
        if ($sharedTables) {
            $this->database->setSharedTables(true)->setTenant(null);
        }
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection('customers', [
            Attribute::string(key: 'name', size: 64),
            Attribute::integer(key: 'score'),
            Attribute::string(key: 'tags', size: 16, array: true),
        ]);
        $this->createCollection('notes', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'body', size: 256),
            Attribute::integer(key: 'score'),
            Attribute::string(key: 'tags', size: 16, array: true),
        ]);
        $this->createCollection('replies', [
            Attribute::string(key: 'noteId', size: 64),
            Attribute::string(key: 'text', size: 256),
        ]);
        $this->createCollection('secrets', [
            Attribute::string(key: 'holderId', size: 64),
            Attribute::string(key: 'secret', size: 64, filters: ['encrypt']),
        ]);

        if ($sharedTables) {
            $this->database->setTenant(self::TENANT);
        }

        $this->createDocument('customers', 'c1', ['name' => 'One', 'score' => 10, 'tags' => ['a']]);
        $this->createDocument('customers', 'c2', ['name' => 'Two', 'score' => 20, 'tags' => ['b']]);
        $this->createDocument('customers', 'c3', ['name' => 'Three', 'score' => 30, 'tags' => []]);
        $this->createDocument('notes', 'n1', ['customerId' => 'c1', 'body' => 'first', 'score' => 1, 'tags' => ['x']]);
        $this->createDocument('notes', 'n2', ['customerId' => 'c1', 'body' => 'second', 'score' => 2, 'tags' => []]);
        $this->createDocument('notes', 'n3', ['customerId' => 'c2', 'body' => 'third', 'score' => 3, 'tags' => []]);
        $this->createDocument('notes', 'n4', ['customerId' => 'ghost', 'body' => 'stray', 'score' => 4, 'tags' => []]);
        $this->createDocument('replies', 'r1', ['noteId' => 'n1', 'text' => 'thanks']);
    }

    /**
     * persons.library is the parent side of a one-to-one relationship, so it holds a column and
     * libraries.person does not; books.owner is the child side of a one-to-many relationship, so it
     * holds a column and persons.books does not.
     */
    private function useRelationships(): void
    {
        $this->database->addHook(new Relationships($this->database));

        $this->createCollection('libraries', [Attribute::string(key: 'name', size: 64)]);
        $this->createCollection('persons', [Attribute::string(key: 'name', size: 64)]);
        $this->createCollection('books', [Attribute::string(key: 'title', size: 64)]);
        $this->database->createRelationship(Relationship::oneToOne(collection: 'persons', relatedCollection: 'libraries', key: 'library', twoWayKey: 'person'));
        $this->database->createRelationship(Relationship::oneToMany(collection: 'persons', relatedCollection: 'books', key: 'books', twoWayKey: 'owner'));

        $this->createDocument('libraries', 'central', ['name' => 'Central']);
        $this->createDocument('persons', 'ada', ['name' => 'Ada', 'library' => 'central']);
        $this->createDocument('persons', 'bob', ['name' => 'Bob']);
        $this->createDocument('books', 'b1', ['title' => 'One', 'owner' => 'ada']);
        $this->createDocument('books', 'b2', ['title' => 'Two', 'owner' => 'ada']);
    }

    /**
     * @param  array<Attribute>  $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(new Collection(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createDocument(string $collection, string $id, array $attributes): void
    {
        $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            ...$attributes,
        ]));
    }
}
