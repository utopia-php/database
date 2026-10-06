<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoSharedTablesInsertTest extends TestCase
{
    public function testCreateDocumentReturnsTheCreatingTenantsDocumentForAnIdAnotherTenantHolds(): void
    {
        $adapter = $this->adapter(sharedTables: true);
        $collection = new Document(['$id' => 'users']);

        $adapter->setTenant(1);
        $first = $adapter->createDocument($collection, $this->user(1, 'first@tenant', 'first'));

        $adapter->setTenant(2);
        $second = $adapter->createDocument($collection, $this->user(2, 'second@tenant', 'second'));

        $this->assertSame(2, $second->getTenant());
        $this->assertSame('second@tenant', $second->getAttribute('email'));
        $this->assertSame('second', $second->getAttribute('secret'));
        $this->assertNotSame($first->getSequence(), $second->getSequence());

        $this->assertSame(1, $first->getTenant());
        $this->assertSame('first@tenant', $first->getAttribute('email'));
    }

    public function testCreateDocumentReturnsTheCreatedDocumentWithoutSharedTables(): void
    {
        $adapter = $this->adapter(sharedTables: false);

        $created = $adapter->createDocument(new Document(['$id' => 'users']), new Document([
            '$id' => 'alice',
            '$permissions' => [],
            'email' => 'only@tenant',
        ]));

        $this->assertSame('only@tenant', $created->getAttribute('email'));
        $this->assertNotSame('', $created->getSequence());
    }

    private function user(int $tenant, string $email, string $secret): Document
    {
        return new Document([
            '$id' => 'alice',
            '$tenant' => $tenant,
            '$permissions' => [],
            'email' => $email,
            'secret' => $secret,
        ]);
    }

    private function adapter(bool $sharedTables): Mongo
    {
        $client = new class () extends Client {
            /**
             * @var array<string, list<array<string, mixed>>>
             */
            private array $rows = [];

            public function __construct()
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            #[\Override]
            public function isReplicaSet(): bool
            {
                return false;
            }

            /**
             * @param  array<string, mixed>  $document
             * @param  array<mixed>  $options
             * @return array<string, mixed>
             */
            #[\Override]
            public function insert(string $collection, array $document, array $options = []): array
            {
                $document['_id'] ??= $this->createUuid();
                $this->rows[$collection][] = $document;

                return $document;
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                $matches = [];
                foreach ($this->rows[$collection] ?? [] as $row) {
                    foreach ($filters as $field => $value) {
                        if (($row[$field] ?? null) !== $value) {
                            continue 2;
                        }
                    }
                    $matches[] = (object) $row;
                }

                return (object) ['cursor' => (object) ['firstBatch' => $matches, 'id' => 0]];
            }
        };

        $authorization = new Authorization();
        $authorization->disable();

        $adapter = new Mongo($client);
        $adapter->setAuthorization($authorization);
        $adapter->setNamespace('tenants');
        $adapter->setSharedTables($sharedTables);

        return $adapter;
    }
}
