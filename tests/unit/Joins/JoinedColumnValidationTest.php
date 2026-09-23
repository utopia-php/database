<?php

namespace Tests\Unit\Joins;

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
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * A joined column is valid exactly when the column would be valid unaliased on the joined
 * collection for the same query type, in find(), count(), sum() and getDocument(), and the
 * arithmetic and bitwise aggregates type a joined attribute by the collection that declares it.
 */
final class JoinedColumnValidationTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('joined_columns')
            ->setNamespace('joined_columns_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection('customers', [
            Attribute::string(key: 'name', size: 64),
            Attribute::integer(key: 'visits'),
        ]);
        $this->createCollection('notes', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'body', size: 256),
            Attribute::integer(key: 'score'),
            Attribute::double(key: 'ratio'),
            Attribute::string(key: 'tags', size: 32, array: true),
            Attribute::integer(key: 'points', array: true),
        ]);
        $this->createCollection('replies', [
            Attribute::string(key: 'noteId', size: 64),
            Attribute::string(key: 'text', size: 256),
        ]);

        $this->createDocument('customers', 'first', ['name' => 'First', 'visits' => 1]);
        $this->createDocument('notes', 'note', [
            'customerId' => 'first',
            'body' => 'needle',
            'score' => 3,
            'ratio' => 0.5,
            'tags' => ['a'],
            'points' => [1],
        ]);
        $this->createDocument('replies', 'reply', ['noteId' => 'note', 'text' => 'thanks']);
    }

    /**
     * @return array<string, array{0: Closure(Database): mixed}>
     */
    public static function unknownJoinedColumnProvider(): array
    {
        return [
            'filter' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::equal('note.nothing', ['x'])])],
            'select' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::select(['name', 'note.nothing'])])],
            'order' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::orderAsc('note.nothing')])],
            'count aggregate' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::count('note.nothing', 'total')])],
            'sum aggregate' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::sum('note.nothing', 'total')])],
            'groupBy' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::count('*', 'rows'), Query::groupBy(['note.nothing'])])],
            'count() filter' => [static fn (Database $database): mixed => $database->count('customers', [self::join(), Query::equal('note.nothing', ['x'])])],
            'sum() filter' => [static fn (Database $database): mixed => $database->sum('customers', 'visits', [self::join(), Query::equal('note.nothing', ['x'])])],
            'getDocument() join condition' => [static fn (Database $database): mixed => $database->getDocument('customers', 'first', [
                Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::equal('note.nothing', ['x'])]),
            ])],
            'getDocument() select' => [static fn (Database $database): mixed => $database->getDocument('customers', 'first', [
                Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')]),
                Query::select(['name', 'note.nothing']),
            ])],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('unknownJoinedColumnProvider')]
    public function testUnknownJoinedColumnIsRejectedBeforeTheEngine(Closure $read): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute not found in schema: note.nothing');

        $read($this->database);
    }

    /**
     * @return array<string, array{0: Closure(Database): mixed, 1: string}>
     */
    public static function internalAttributeOutsideItsQueryTypeProvider(): array
    {
        return [
            '$permissions in a filter' => [
                static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::equal('note.$permissions', ['x'])]),
                'Invalid query: Attribute not found in schema: note.$permissions',
            ],
            '$permissions in an order' => [
                static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::orderAsc('note.$permissions')]),
                'Invalid query: Attribute not found in schema: note.$permissions',
            ],
            '$tenant in a filter' => [
                static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::isNotNull('note.$tenant')]),
                'Invalid query: Attribute not found in schema: note.$tenant',
            ],
            '$collection in a select' => [
                static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::select(['name', 'note.$collection'])]),
                'Invalid query: Attribute not found in schema: note.$collection',
            ],
            '$permissions in a getDocument() join condition' => [
                static fn (Database $database): mixed => $database->getDocument('customers', 'first', [
                    Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::equal('note.$permissions', ['x'])]),
                ]),
                'Invalid query: Attribute not found in schema: note.$permissions',
            ],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('internalAttributeOutsideItsQueryTypeProvider')]
    public function testJoinedInternalAttributeIsRejectedWhereTheMainCollectionRejectsIt(Closure $read, string $message): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage($message);

        $read($this->database);
    }

    public function testGetDocumentChecksJoinConditionValuesLikeFind(): void
    {
        $queries = [
            Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::equal('name', [5])]),
        ];

        foreach ([
            'find' => fn (): mixed => $this->database->find('customers', $queries),
            'getDocument' => fn (): mixed => $this->database->getDocument('customers', 'first', $queries),
        ] as $method => $read) {
            try {
                $read();
                $this->fail($method.'() accepted a join condition whose value does not fit the attribute');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Query value is invalid for attribute "name"', $error->getMessage(), $method);
            }
        }
    }

    /**
     * @return array<string, array{0: Closure(Database): mixed}>
     */
    public static function validJoinedColumnProvider(): array
    {
        return [
            'known column in a filter' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::equal('note.body', ['needle'])])],
            'known column in a select' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::select(['name', 'note.body'])])],
            'known column in an order' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::orderAsc('note.body')])],
            'known column in a count' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::count('note.body', 'total')])],
            'known column in a sum' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::sum('note.score', 'total')])],
            'known column in a groupBy' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::count('*', 'rows'), Query::groupBy(['note.body'])])],
            'known column in a between' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::between('note.score', 1, 5)])],
            '$id in a filter' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::equal('note.$id', ['note'])])],
            '$id in a select' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::select(['name', 'note.$id'])])],
            '$id in a count' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::count('note.$id', 'notes')])],
            '$createdAt in a between' => [static fn (Database $database): mixed => $database->find('customers', [self::join(), Query::between('note.$createdAt', '1970-01-01', '2099-12-31')])],
            '$permissions, $createdAt, $updatedAt and $sequence in a select' => [static fn (Database $database): mixed => $database->find('customers', [
                self::join(),
                Query::select(['name', 'note.$permissions', 'note.$createdAt', 'note.$updatedAt', 'note.$sequence']),
            ])],
            'join chained on $id' => [static fn (Database $database): mixed => $database->find('customers', [
                self::join(),
                Query::join('replies', 'note.$id', 'noteId', '=', 'reply'),
                Query::select(['name', 'reply.text']),
            ])],
            'count() with a known column' => [static fn (Database $database): mixed => $database->count('customers', [self::join(), Query::equal('note.body', ['needle'])])],
            'sum() with a known column' => [static fn (Database $database): mixed => $database->sum('customers', 'visits', [self::join(), Query::equal('note.body', ['needle'])])],
            'getDocument() join condition on a known column' => [static fn (Database $database): mixed => $database->getDocument('customers', 'first', [
                Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::equal('note.body', ['needle'])]),
            ])],
            'getDocument() select of joined columns' => [static fn (Database $database): mixed => $database->getDocument('customers', 'first', [
                Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')]),
                Query::select(['name', 'note.body', 'note.$id', 'note.$permissions']),
            ])],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('validJoinedColumnProvider')]
    public function testJoinedColumnValidOnTheJoinedCollectionStaysValid(Closure $read): void
    {
        $result = $read($this->database);

        if ($result instanceof Document) {
            $this->assertSame('first', $result->getId());
        } elseif (\is_array($result)) {
            $this->assertCount(1, $result);
        } else {
            $this->assertSame(1, $result);
        }
    }

    public function testTopLevelFilterInGetDocumentStaysAnInvalidMethod(): void
    {
        foreach ([
            'without a join' => [Query::equal('name', ['First'])],
            'with a join' => [Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')]), Query::equal('name', ['First'])],
        ] as $label => $queries) {
            try {
                $this->database->getDocument('customers', 'first', $queries);
                $this->fail('getDocument() accepted a top-level filter '.$label);
            } catch (QueryException $error) {
                $this->assertSame('Invalid query method: equal', $error->getMessage(), $label);
            }
        }
    }

    /**
     * @return array<string, array{0: Query, 1: string}>
     */
    public static function nonNumericJoinedAttributeProvider(): array
    {
        $numeric = static fn (string $method, string $attribute): string => 'Invalid query: Aggregate '.$method.' requires a numeric attribute that is not an array: '.$attribute;
        $integer = static fn (string $method, string $attribute): string => 'Invalid query: Aggregate '.$method.' requires an integer attribute that is not an array: '.$attribute;

        return [
            'sum of a joined string' => [Query::sum('note.body', 'result'), $numeric('sum', 'note.body')],
            'sum of a bare name resolved to a joined string' => [Query::sum('body', 'result'), $numeric('sum', 'body')],
            'avg of a joined string' => [Query::avg('note.body', 'result'), $numeric('avg', 'note.body')],
            'stddev of a joined string' => [Query::stddev('note.body', 'result'), $numeric('stddev', 'note.body')],
            'variance of a bare name resolved to a joined string' => [Query::variance('body', 'result'), $numeric('variance', 'body')],
            'bitAnd of a joined string' => [Query::bitAnd('note.body', 'result'), $numeric('bitAnd', 'note.body')],
            'sum of a joined string array' => [Query::sum('note.tags', 'result'), $numeric('sum', 'note.tags')],
            'avg of a bare name resolved to a joined string array' => [Query::avg('tags', 'result'), $numeric('avg', 'tags')],
            'sum of a joined integer array' => [Query::sum('note.points', 'result'), $numeric('sum', 'note.points')],
            'bitOr of a bare name resolved to a joined integer array' => [Query::bitOr('points', 'result'), $numeric('bitOr', 'points')],
            'bitXor of a joined double' => [Query::bitXor('note.ratio', 'result'), $integer('bitXor', 'note.ratio')],
            'sum of a joined internal attribute' => [Query::sum('note.$createdAt', 'result'), $numeric('sum', 'note.$createdAt')],
        ];
    }

    #[DataProvider('nonNumericJoinedAttributeProvider')]
    public function testArithmeticAndBitwiseAggregatesTypeAJoinedAttributeByItsCollection(Query $aggregate, string $message): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage($message);

        $this->database->find('customers', [self::join(), $aggregate]);
    }

    public function testArithmeticAggregatesOfNumericJoinedAttributesStayValid(): void
    {
        $results = $this->database->find('customers', [
            self::join(),
            Query::sum('note.score', 'total'),
            Query::avg('note.ratio', 'average'),
            Query::sum('score', 'bare'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(3, $results[0]->getAttribute('total'));
        $this->assertSame(0.5, $results[0]->getAttribute('average'));
        $this->assertSame(3, $results[0]->getAttribute('bare'));
    }

    private static function join(): Query
    {
        return Query::join('notes', '$id', 'customerId', '=', 'note');
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
