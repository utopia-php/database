<?php

namespace Tests\Unit\Validator;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
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
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Query\Join;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;

/**
 * The query validators refuse the query shapes the library cannot run as written, so find(), count()
 * and sum() report them as `Exception\Query` before any statement is built.
 */
final class QueryValidationTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('query_validation')
            ->setNamespace('query_validation_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->addHook(new Relationships($this->database));
        $this->database->create();

        $this->createCollection('authors', [Attribute::string(key: 'name', size: 32)]);
        $this->createCollection('users', [Attribute::string(key: 'name', size: 32)]);
        $this->createCollection('posts', [Attribute::string(key: 'owner', size: 32), Attribute::integer(key: 'votes')]);
        $this->database->createRelationship(new Relationship(
            collection: 'posts',
            relatedCollection: 'authors',
            type: RelationType::ManyToOne,
            key: 'author',
            onDelete: ForeignKeyAction::SetNull,
        ));

        $this->createDocument('authors', 'ann', ['name' => 'Ann']);
        $this->createDocument('authors', 'bob', ['name' => 'Bob']);
        $this->createDocument('users', 'bob', ['name' => 'Bob']);
        $this->createDocument('users', 'ann', ['name' => 'Ann']);
        $this->createDocument('posts', 'first', ['owner' => 'bob', 'votes' => 3, 'author' => 'ann']);
        $this->createDocument('posts', 'second', ['owner' => 'ann', 'votes' => 5, 'author' => 'bob']);

        $this->createCollection('owners', [
            Attribute::string(key: 'name', size: 32),
            Attribute::integer(key: 'score'),
            Attribute::boolean(key: 'active'),
            Attribute::string(key: 'tags', size: 32, array: true),
        ]);
        $this->createCollection('items', [
            Attribute::string(key: 'title', size: 32),
            Attribute::integer(key: 'price'),
            Attribute::boolean(key: 'featured'),
            Attribute::string(key: 'labels', size: 32, array: true),
            Attribute::string(key: 'ownerRef', size: 32),
        ]);
        $this->database->createRelationship(new Relationship(
            collection: 'owners',
            relatedCollection: 'items',
            type: RelationType::OneToMany,
            twoWay: true,
            key: 'items',
            twoWayKey: 'owner',
            onDelete: ForeignKeyAction::SetNull,
        ));

        $this->createDocument('owners', 'ann', ['name' => 'Ann', 'score' => 2, 'active' => true, 'tags' => ['a']]);
        $this->createDocument('owners', 'bob', ['name' => 'Bob', 'score' => 4, 'active' => false, 'tags' => ['b']]);
        $this->createDocument('items', 'pen', ['title' => 'pen', 'price' => 5, 'featured' => true, 'labels' => ['x'], 'ownerRef' => 'ann', 'owner' => 'ann']);
        $this->createDocument('items', 'cup', ['title' => 'cup', 'price' => 7, 'featured' => false, 'labels' => ['y'], 'ownerRef' => 'bob', 'owner' => 'bob']);
    }

    /**
     * With a join aliased `author` next to the relationship `author`, `author.name` would be checked
     * against the joined users but run as a filter on the related authors, returning the other post.
     */
    public function testAJoinAliasEqualToARelationshipKeyIsRejected(): void
    {
        $message = 'Join alias "author" is the key of the relationship attribute "author": give the join another alias';

        foreach ([
            'inline condition' => Query::join('users', 'owner', '$id', '=', 'author'),
            'on() condition' => Query::join('users', 'author', [Query::on('owner', '$id')]),
            'left join' => Query::leftJoin('users', 'owner', '$id', '=', 'author'),
        ] as $shape => $join) {
            $queries = [$join, Query::equal('author.name', ['Bob'])];

            $this->assertInvalidQuery($message, fn (): mixed => $this->database->find('posts', $queries), $shape.': find()');
            $this->assertInvalidQuery($message, fn (): mixed => $this->database->count('posts', $queries), $shape.': count()');
            $this->assertInvalidQuery($message, fn (): mixed => $this->database->sum('posts', 'votes', $queries), $shape.': sum()');
            $this->assertInvalidQuery($message, fn (): mixed => $this->database->getDocument('posts', 'first', [$join]), $shape.': getDocument()');
        }

        $this->assertInvalidQuery(
            $message,
            fn (): mixed => $this->database->find('posts', [Query::join('users', 'owner', '$id', '=', 'author'), Query::count('*', 'rows'), Query::select(['author.*'])]),
            'an aggregate next to the relationship wildcard',
        );

        $joined = $this->database->find('posts', [Query::join('users', 'owner', '$id', '=', 'usr'), Query::equal('usr.name', ['Bob'])]);
        $this->assertSame(['first'], $this->ids($joined), 'another alias filters the joined users');
        $this->assertSame(1, $this->database->count('posts', [Query::join('users', 'owner', '$id', '=', 'usr'), Query::equal('usr.name', ['Bob'])]));
        $this->assertSame(['second'], $this->ids($this->database->find('posts', [Query::equal('author.name', ['Bob'])])), 'without a join author.name filters the related authors');
    }

    public function testTheJoinValidatorNamesTheRelationshipAnAliasCollidesWith(): void
    {
        $validator = new Join([
            new Document(['$id' => 'owner', 'key' => 'owner', 'type' => ColumnType::String->value]),
            new Document(['$id' => 'author', 'key' => 'author', 'type' => ColumnType::Relationship->value, 'options' => ['relationType' => RelationType::ManyToOne->value, 'side' => 'parent', 'relatedCollection' => 'authors']]),
        ]);

        $this->assertFalse($validator->isValid(Query::join('users', 'owner', '$id', '=', 'author')));
        $this->assertSame('Join alias "author" is the key of the relationship attribute "author": give the join another alias', $validator->getDescription());

        $validator->resetJoinAliases();
        $this->assertTrue($validator->isValid(Query::join('users', 'owner', '$id', '=', 'Author')), 'relationship keys are matched as the relationship hook matches them, by exact name');

        $validator->resetJoinAliases();
        $this->assertTrue($validator->isValid(Query::join('users', 'owner', '$id', '=', 'owner')), 'an alias may still equal an attribute that is not a relationship');
    }

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function refusedJoinConditions(): iterable
    {
        yield 'limit' => [Query::limit(1), 'limit'];
        yield 'offset' => [Query::offset(1), 'offset'];
        yield 'cursor' => [Query::cursorAfter(new Document(['$id' => 'pen'])), 'cursorAfter'];
        yield 'order' => [Query::orderAsc('title'), 'orderAsc'];
        yield 'select' => [Query::select(['title']), 'select'];
        yield 'aggregate' => [Query::count('*', 'rows'), 'count'];
        yield 'join' => [Query::join('owners', '$id', '$id', '=', 'nested'), 'join'];
        yield 'containsAll' => [Query::containsAll('it.labels', ['x']), 'containsAll'];
        yield 'search' => [Query::search('it.title', 'pen'), 'search'];
        yield 'regex' => [Query::regex('it.title', '^p'), 'regex'];
        yield 'regex inside or()' => [Query::or([Query::equal('it.title', ['pen']), Query::regex('it.title', '^p')]), 'regex'];
    }

    /**
     * The builder compiles a join's ON list from on() conditions and plain filters only, and refuses
     * the rest while it builds the statement.
     */
    #[DataProvider('refusedJoinConditions')]
    public function testAJoinOnListAcceptsOnlyConditionsAndPlainFilters(Query $condition, string $method): void
    {
        $queries = [Query::join('items', 'it', [Query::on('$id', 'ownerRef'), $condition])];
        $message = 'Unsupported join ON condition: '.$method;

        $this->assertInvalidQuery($message, fn (): mixed => $this->database->find('owners', $queries), 'find()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->count('owners', $queries), 'count()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->sum('owners', 'score', $queries), 'sum()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->getDocument('owners', 'ann', $queries), 'getDocument()');
    }

    public function testAJoinOnListRunsItsPlainFilters(): void
    {
        foreach ([
            'equal' => Query::equal('it.title', ['pen']),
            'or()' => Query::or([Query::equal('it.title', ['pen']), Query::startsWith('it.title', 'pe')]),
            'contains on an array' => Query::contains('it.labels', ['x']),
        ] as $shape => $filter) {
            $queries = [Query::join('items', 'it', [Query::on('$id', 'ownerRef'), $filter])];

            $this->assertSame(['ann'], $this->ids($this->database->find('owners', $queries)), $shape);
            $this->assertSame(1, $this->database->count('owners', $queries), $shape);
            $this->assertSame(2, $this->database->sum('owners', 'score', $queries), $shape);
        }
    }

    /**
     * @return iterable<string, array{Query, string}>
     */
    public static function refusedExistsQueries(): iterable
    {
        yield 'an internal column next to an attribute' => [new Query(Method::Exists, 'name', ['_permissions']), 'Attribute not found in schema: _permissions'];
        yield 'an internal column' => [Query::exists(['_uid']), 'Attribute not found in schema: _uid'];
        yield 'an internal column, notExists' => [Query::notExists(['_permissions']), 'Attribute not found in schema: _permissions'];
        yield 'an unknown attribute' => [Query::exists(['name', 'missing']), 'Attribute not found in schema: missing'];
        yield 'a relationship side without a column' => [Query::exists(['items']), 'Cannot query on virtual relationship attribute'];
        yield 'a related document\'s attribute' => [Query::exists(['items.title']), 'Exists queries take attributes of the collection or of a join alias: items.title'];
        yield 'a value that is not a name' => [Query::notExists([7]), 'NotExists queries take attribute names'];
    }

    /**
     * exists() and notExists() test the columns their values name, so each value has to name an
     * attribute a filter could name.
     */
    #[DataProvider('refusedExistsQueries')]
    public function testExistsValuesNameAttributesOfTheCollection(Query $query, string $message): void
    {
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->find('owners', [$query]), 'find()');
        $this->assertInvalidQuery($message, fn (): mixed => $this->database->count('owners', [$query]), 'count()');
    }

    public function testExistsInItsDocumentedFormRuns(): void
    {
        $this->assertSame(['ann', 'bob'], $this->ids($this->database->find('owners', [Query::exists(['name'])])));
        $this->assertSame(['ann', 'bob'], $this->ids($this->database->find('owners', [Query::exists(['name', 'score', '$createdAt'])])));
        $this->assertSame([], $this->ids($this->database->find('owners', [Query::notExists('name')])));
        $this->assertSame(2, $this->database->count('owners', [Query::exists(['tags'])]));
        $this->assertSame(['pen', 'cup'], $this->ids($this->database->find('items', [Query::exists(['owner'])])), 'the side of a relationship that holds a column');

        $joined = [Query::join('items', '$id', 'ownerRef', '=', 'it'), Query::exists(['it.title'])];
        $this->assertSame(['ann', 'bob'], $this->ids($this->database->find('owners', $joined)), 'a column under a join alias');
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(static fn (Document $document): string => $document->getId(), \array_values($documents));
    }

    private function assertInvalidQuery(string $message, Closure $run, string $case = ''): void
    {
        try {
            $run();
            $this->fail($case.': the query ran instead of being rejected with "'.$message.'"');
        } catch (QueryException $error) {
            $this->assertStringContainsString($message, $error->getMessage(), $case);
        }
    }

    /**
     * @param  array<Attribute>  $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(new Collection(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
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
