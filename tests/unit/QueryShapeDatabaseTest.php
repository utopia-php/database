<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
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

final class QueryShapeDatabaseTest extends TestCase
{
    private const string COLLECTION = 'orders';

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $database
            ->setDatabase('query_shape')
            ->setNamespace('query_shape_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'amount', required: true),
                Attribute::double(key: 'rating', default: 0.0),
                Attribute::string(key: 'status', size: 20, required: true),
                Attribute::string(key: 'body', size: 200, default: ''),
                Attribute::boolean(key: 'paid', default: false),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach ([[5, 'paid'], [7, 'paid'], [3, 'open']] as [$amount, $status]) {
            $database->createDocument(self::COLLECTION, new Document([
                'amount' => $amount,
                'rating' => $amount / 2,
                'status' => $status,
                'body' => 'order of '.$amount,
                'paid' => $status === 'paid',
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        return $database;
    }

    /**
     * @return list<Query>
     */
    private static function crossJoins(int $count): array
    {
        return \array_map(fn (int $index): Query => Query::crossJoin(self::COLLECTION, 'joined'.$index), \range(1, $count));
    }

    /**
     * @return array<string, array{0: list<Query>, 1: string}>
     */
    public static function rejectedShapeProvider(): array
    {
        return [
            'more joins than allowed' => [
                self::crossJoins(9),
                'Too many joins: at most 8 are allowed',
            ],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('rejectedShapeProvider')]
    public function testFindRejectsTheShapeWithAQueryException(array $queries, string $message): void
    {
        $error = $this->capture(fn () => $this->database()->find(self::COLLECTION, $queries));

        $this->assertInstanceOf(QueryException::class, $error, $error === null ? 'find() accepted the query shape' : $error::class.': '.$error->getMessage());
        $this->assertSame($message, $error->getMessage());
    }

    private function capture(callable $call): ?Throwable
    {
        try {
            $call();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }

    public function testCountAndSumRejectTooManyJoins(): void
    {
        $database = $this->database();

        $this->assertSame(3, $database->count(self::COLLECTION, []));

        foreach ([
            'count' => fn () => $database->count(self::COLLECTION, self::crossJoins(9)),
            'sum' => fn () => $database->sum(self::COLLECTION, 'amount', self::crossJoins(9)),
        ] as $method => $call) {
            $error = $this->capture($call);

            $this->assertInstanceOf(QueryException::class, $error, $error === null ? $method.'() accepted nine joins' : $error::class.': '.$error->getMessage());
            $this->assertSame('Too many joins: at most 8 are allowed', $error->getMessage());
        }
    }

    public function testEightJoinsAreStillAllowed(): void
    {
        $database = $this->database();

        $rows = $database->find(self::COLLECTION, [
            Query::equal('status', ['open']),
            ...self::crossJoins(8),
            Query::limit(1),
        ]);

        $this->assertCount(1, $rows);
    }
}
