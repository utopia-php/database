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
 * collection for the same query type, in find(), count() and sum().
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
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('validJoinedColumnProvider')]
    public function testJoinedColumnValidOnTheJoinedCollectionStaysValid(Closure $read): void
    {
        $result = $read($this->database);

        if (\is_array($result)) {
            $this->assertCount(1, $result);
        } else {
            $this->assertSame(1, $result);
        }
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
