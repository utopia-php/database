<?php

namespace Tests\Unit\Joins;

use PDO;
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
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\IndexType;

/**
 * A bare aggregate or groupBy attribute names the main collection's attribute when the main
 * collection declares it, otherwise the attribute of the one joined collection that does; and a
 * search on a joined attribute needs a fulltext index on the joined collection.
 */
final class JoinedAttributeResolutionTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase('joined_attributes')
            ->setNamespace('joined_attributes_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->createCollection('customers', [
            Attribute::string(key: 'name', size: 64),
            Attribute::integer(key: 'visits'),
        ]);
        $this->createCollection('orders', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::integer(key: 'amount'),
            Attribute::string(key: 'status', size: 32),
            Attribute::string(key: 'memo', size: 256),
        ]);
        $this->createCollection('refunds', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::integer(key: 'amount'),
        ]);
        $this->createCollection('notes', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::string(key: 'body', size: 256),
        ], [
            new Index(key: 'body_fulltext', type: IndexType::Fulltext, attributes: ['body']),
        ]);
        $this->createCollection('profiles', [
            Attribute::string(key: 'customerId', size: 64),
            Attribute::integer(key: 'visits'),
        ]);

        $this->createDocument('customers', 'first', ['name' => 'First', 'visits' => 1]);
        $this->createDocument('customers', 'second', ['name' => 'Second', 'visits' => 2]);
        $this->createDocument('orders', 'paid', ['customerId' => 'first', 'amount' => 100, 'status' => 'paid', 'memo' => 'gift wrapped']);
        $this->createDocument('orders', 'open', ['customerId' => 'first', 'amount' => 50, 'status' => 'open', 'memo' => 'pending']);
        $this->createDocument('orders', 'other', ['customerId' => 'second', 'amount' => 7, 'status' => 'paid', 'memo' => 'plain']);
        $this->createDocument('refunds', 'refund', ['customerId' => 'first', 'amount' => 5]);
        $this->createDocument('notes', 'note', ['customerId' => 'first', 'body' => 'a needle in a haystack']);
        $this->createDocument('profiles', 'profile', ['customerId' => 'first', 'visits' => 1000]);
    }

    public function testBareAttributesNoCollectionDeclaresAreRejected(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute not found in schema: anything_at_all');

        $this->database->find('customers', [
            Query::leftJoin('orders', '$id', 'customerId', '=', 'j'),
            Query::groupBy(['anything_at_all']),
            Query::sum('also_anything', 'total'),
        ]);
    }

    public function testBareAggregateAttributeSeveralJoinsDeclareIsRejectedAsAmbiguous(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute "amount" is ambiguous across joins; qualify it with a join alias');

        $this->database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'alpha'),
            Query::join('refunds', '$id', 'customerId', '=', 'beta'),
            Query::sum('amount', 'total'),
        ]);
    }

    public function testBareGroupByAttributeSeveralJoinsDeclareIsRejectedAsAmbiguous(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute "amount" is ambiguous across joins; qualify it with a join alias');

        $this->database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'alpha'),
            Query::join('refunds', '$id', 'customerId', '=', 'beta'),
            Query::count('*', 'rows'),
            Query::groupBy(['amount']),
        ]);
    }

    public function testQualifiedAttributesStillPickTheirJoin(): void
    {
        $results = $this->database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'alpha'),
            Query::join('refunds', '$id', 'customerId', '=', 'beta'),
            Query::sum('alpha.amount', 'ordered'),
            Query::sum('beta.amount', 'refunded'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(150, $results[0]->getAttribute('ordered'));
        $this->assertSame(10, $results[0]->getAttribute('refunded'));
    }

    public function testBareAggregateAttributeResolvesToTheOneJoinThatDeclaresIt(): void
    {
        $results = $this->database->find('customers', [
            Query::join('notes', '$id', 'customerId', '=', 'note'),
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(150, $results[0]->getAttribute('total'));
    }

    public function testBareGroupByAttributeResolvesToTheOneJoinThatDeclaresIt(): void
    {
        $results = $this->database->find('customers', [
            Query::join('notes', '$id', 'customerId', '=', 'note'),
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
            Query::groupBy(['status']),
        ]);

        $totals = [];
        foreach ($results as $result) {
            $status = $result->getAttribute('status');
            $this->assertIsString($status);
            $totals[$status] = $result->getAttribute('total');
        }
        \ksort($totals);

        $this->assertSame(['open' => 50, 'paid' => 100], $totals);
    }

    public function testBareAttributeResolvesThroughJoinsWithoutAliases(): void
    {
        $results = $this->database->find('customers', [
            Query::join('notes', '$id', 'customerId'),
            Query::join('orders', '$id', 'customerId'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(150, $results[0]->getAttribute('total'));
    }

    public function testBareAttributeOfTheMainCollectionIsNotReboundToAJoin(): void
    {
        $results = $this->database->find('customers', [
            Query::join('profiles', '$id', 'customerId', '=', 'profile'),
            Query::sum('visits', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(1, $results[0]->getAttribute('total'));
    }

    public function testBareInternalAttributeResolvesToTheMainCollection(): void
    {
        $results = $this->database->find('customers', [
            Query::leftJoin('notes', '$id', 'customerId', '=', 'note'),
            Query::count('$id', 'customers'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(2, $results[0]->getAttribute('customers'));
    }

    public function testAdapterResolvesBareAttributesTheSameWayWithoutValidation(): void
    {
        $results = $this->database->skipValidation(fn () => $this->database->find('customers', [
            Query::join('notes', '$id', 'customerId', '=', 'note'),
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
        ]));
        $this->assertSame(150, $results[0]->getAttribute('total'));

        foreach ([
            'Attribute "amount" is ambiguous across joins; qualify it with a join alias' => [
                Query::join('orders', '$id', 'customerId', '=', 'alpha'),
                Query::join('refunds', '$id', 'customerId', '=', 'beta'),
                Query::sum('amount', 'total'),
            ],
            'Attribute not found in schema: also_anything' => [
                Query::leftJoin('orders', '$id', 'customerId', '=', 'j'),
                Query::sum('also_anything', 'total'),
            ],
        ] as $message => $queries) {
            try {
                $this->database->skipValidation(fn () => $this->database->find('customers', $queries));
                $this->fail('The adapter bound a bare attribute it could not resolve: '.$message);
            } catch (QueryException $error) {
                $this->assertSame($message, $error->getMessage());
            }
        }
    }

    public function testSearchOnAJoinedAttributeWithoutAFulltextIndexIsRejected(): void
    {
        $queries = [
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::search('purchase.memo', 'gift'),
        ];

        foreach ([
            'find' => fn () => $this->database->find('customers', $queries),
            'count' => fn () => $this->database->count('customers', $queries),
            'sum' => fn () => $this->database->sum('customers', 'visits', $queries),
        ] as $method => $read) {
            try {
                $read();
                $this->fail($method.'() accepted a search on a joined attribute without a fulltext index');
            } catch (QueryException $error) {
                $this->assertSame('Searching by attribute "purchase.memo" requires a fulltext index.', $error->getMessage(), $method);
            }
        }
    }

    public function testSearchOnAJoinedAttributeWithAFulltextIndexIsAccepted(): void
    {
        $queries = [
            Query::join('notes', '$id', 'customerId', '=', 'note'),
            Query::search('note.body', 'needle'),
        ];

        $results = $this->database->find('customers', [...$queries, Query::select(['name'])]);

        $this->assertSame(['first'], \array_map(static fn (Document $document): string => $document->getId(), $results));
        $this->assertSame(1, $this->database->count('customers', $queries));
    }

    public function testUnknownJoinedCollectionIsReportedAsNotFound(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage("Joined collection 'missing' not found");

        $this->database->count('customers', [
            Query::join('missing', '$id', 'customerId', '=', 'gone'),
            Query::search('gone.body', 'needle'),
        ]);
    }

    public function testJoinedResolutionDoesNotCarryOverToAFindWithoutJoins(): void
    {
        $joined = $this->database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
        ]);
        $this->assertSame(157, $joined[0]->getAttribute('total'));

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query: Attribute not found in schema: amount');

        $this->database->find('customers', [Query::sum('amount', 'total')]);
    }

    /**
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     */
    private function createCollection(string $id, array $attributes, array $indexes = []): void
    {
        $this->database->createCollection(new Collection(
            id: $id,
            attributes: $attributes,
            indexes: $indexes,
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
