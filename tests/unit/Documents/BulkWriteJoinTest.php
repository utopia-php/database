<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Mirror;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

class BulkWriteJoinTest extends TestCase
{
    /**
     * @return array<string, array{callable(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
            'memory' => [static fn (): Adapter => new Memory()],
        ];
    }

    /**
     * @param  callable(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUpdateDocumentsRejectsJoins(callable $adapter): void
    {
        $database = $this->createDatabase($adapter());

        try {
            $database->updateDocuments('orders', new Document(['amount' => 0]), [
                Query::join('customers', 'customerId', '$id'),
            ]);
            $this->fail('A join on a bulk update must be rejected');
        } catch (QueryException $exception) {
            $this->assertSame('Join queries are not supported for bulk updates', $exception->getMessage());
        }

        $this->assertSame(10, $database->getDocument('orders', 'o1')->getAttribute('amount'));
    }

    /**
     * @param  callable(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeleteDocumentsRejectsJoins(callable $adapter): void
    {
        $database = $this->createDatabase($adapter());

        try {
            $database->deleteDocuments('orders', [
                Query::leftJoin('customers', 'customerId', '$id'),
            ]);
            $this->fail('A join on a bulk delete must be rejected');
        } catch (QueryException $exception) {
            $this->assertSame('Join queries are not supported for bulk deletes', $exception->getMessage());
        }

        $this->assertFalse($database->getDocument('orders', 'o1')->isEmpty());
    }

    /**
     * @param  callable(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testJoinsAreRejectedWithoutQueryValidation(callable $adapter): void
    {
        $database = $this->createDatabase($adapter());

        $database->skipValidation(function () use ($database): void {
            try {
                $database->updateDocuments('orders', new Document(['amount' => 0]), [
                    Query::join('customers', 'customerId', '$id'),
                ]);
                $this->fail('A join on a bulk update must be rejected');
            } catch (QueryException $exception) {
                $this->assertSame('Join queries are not supported for bulk updates', $exception->getMessage());
            }

            try {
                $database->deleteDocuments('orders', [
                    Query::join('customers', 'customerId', '$id'),
                ]);
                $this->fail('A join on a bulk delete must be rejected');
            } catch (QueryException $exception) {
                $this->assertSame('Join queries are not supported for bulk deletes', $exception->getMessage());
            }
        });

        $this->assertSame(10, $database->getDocument('orders', 'o1')->getAttribute('amount'));
    }

    public function testMirrorRejectsJoinsBeforeWritingToTheSource(): void
    {
        $source = $this->createDatabase(new SQLite(new PDO('sqlite::memory:')));
        $mirror = new Mirror($source);

        try {
            $mirror->updateDocuments('orders', new Document(['amount' => 0]), [
                Query::join('customers', 'customerId', '$id'),
            ]);
            $this->fail('A join on a bulk update must be rejected');
        } catch (QueryException $exception) {
            $this->assertSame('Join queries are not supported for bulk updates', $exception->getMessage());
        }

        try {
            $mirror->deleteDocuments('orders', [
                Query::join('customers', 'customerId', '$id'),
            ]);
            $this->fail('A join on a bulk delete must be rejected');
        } catch (QueryException $exception) {
            $this->assertSame('Join queries are not supported for bulk deletes', $exception->getMessage());
        }

        $this->assertSame(10, $source->getDocument('orders', 'o1')->getAttribute('amount'));
    }

    /**
     * @param  callable(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testBulkWritesWithoutJoinsStillApply(callable $adapter): void
    {
        $database = $this->createDatabase($adapter());

        $this->assertSame(1, $database->updateDocuments('orders', new Document(['amount' => 20]), [
            Query::equal('customerId', ['c1']),
        ]));
        $this->assertSame(20, $database->getDocument('orders', 'o1')->getAttribute('amount'));

        $this->assertSame(1, $database->deleteDocuments('orders', [
            Query::equal('customerId', ['c1']),
        ]));
        $this->assertTrue($database->getDocument('orders', 'o1')->isEmpty());
    }

    private function createDatabase(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('bulk_join')
            ->setNamespace('bulk_join_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->create();

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $database->createCollection(new Collection(
            id: 'customers',
            attributes: [Attribute::string(key: 'name')],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: 'orders',
            attributes: [
                Attribute::string(key: 'customerId'),
                Attribute::integer(key: 'amount'),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));

        $database->createDocument('customers', new Document(['$id' => 'c1', 'name' => 'Customer']));
        $database->createDocument('orders', new Document(['$id' => 'o1', 'customerId' => 'c1', 'amount' => 10]));

        return $database;
    }
}
