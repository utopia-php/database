<?php

namespace Tests\Unit\Adapter;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Attribute;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;

final class MongoTenantSchemaTest extends TestCase
{
    private const int TENANT = 7;

    /**
     * @var list<array{collection: string, where: array<mixed>, updates: array<mixed>, multi: bool}>
     */
    private array $updates = [];

    public function testRenameAttributeRenamesOnlyTheTenantsDocuments(): void
    {
        $this->createAdapter(sharedTables: true)->renameAttribute('users', 'nick', 'title');

        $this->assertSame([[
            'collection' => 'scope_users',
            'where' => [Storage::TENANT => self::TENANT],
            'updates' => ['$rename' => ['nick' => 'title']],
            'multi' => true,
        ]], $this->updates);
    }

    public function testUpdateAttributeWithANewKeyRenamesOnlyTheTenantsDocuments(): void
    {
        $this->createAdapter(sharedTables: true)->updateAttribute('users', Attribute::string(key: 'nick', size: 64), 'title');

        $this->assertSame([[
            'collection' => 'scope_users',
            'where' => [Storage::TENANT => self::TENANT],
            'updates' => ['$rename' => ['nick' => 'title']],
            'multi' => true,
        ]], $this->updates);
    }

    public function testDeleteAttributeUnsetsOnlyTheTenantsDocuments(): void
    {
        $this->createAdapter(sharedTables: true)->deleteAttribute('users', 'nick');

        $this->assertSame([[
            'collection' => 'scope_users',
            'where' => [Storage::TENANT => self::TENANT],
            'updates' => ['$unset' => ['nick' => '']],
            'multi' => true,
        ]], $this->updates);
    }

    public function testRelationshipRenamesAndDeletesStayInsideTheTenant(): void
    {
        $adapter = $this->createAdapter(sharedTables: true);
        $relationship = new Relationship(
            collection: 'users',
            relatedCollection: 'profiles',
            type: RelationType::OneToOne,
            twoWay: true,
            key: 'profile',
            twoWayKey: 'user',
            side: RelationSide::Parent,
        );

        $adapter->updateRelationship($relationship, 'account', 'owner');
        $adapter->deleteRelationship($relationship);

        $this->assertSame(
            [
                ['scope_users', ['$rename' => ['profile' => 'account']]],
                ['scope_profiles', ['$rename' => ['user' => 'owner']]],
                ['scope_users', ['$unset' => ['profile' => '']]],
                ['scope_profiles', ['$unset' => ['user' => '']]],
            ],
            \array_map(static fn (array $update): array => [$update['collection'], $update['updates']], $this->updates),
        );
        foreach ($this->updates as $update) {
            $this->assertSame([Storage::TENANT => self::TENANT], $update['where'], 'A relationship change must reach only the selected tenant\'s documents');
        }
    }

    public function testDedicatedTablesChangeEveryDocument(): void
    {
        $adapter = $this->createAdapter(sharedTables: false);

        $adapter->renameAttribute('users', 'nick', 'title');
        $adapter->deleteAttribute('users', 'title');

        $this->assertSame([[], []], \array_map(static fn (array $update): array => $update['where'], $this->updates));
    }

    private function createAdapter(bool $sharedTables): Mongo
    {
        $record = function (string $collection, array $where, array $updates, bool $multi): void {
            $this->updates[] = ['collection' => $collection, 'where' => $where, 'updates' => $updates, 'multi' => $multi];
        };

        $client = new class ($record) extends Client {
            /**
             * @param  Closure(string, array<mixed>, array<mixed>, bool): void  $record
             */
            public function __construct(private readonly Closure $record)
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

            /**
             * @param  array<mixed>  $where
             * @param  array<mixed>  $updates
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function update(string $collection, array $where = [], array $updates = [], array $options = [], bool $multi = false): int
            {
                ($this->record)($collection, $where, $updates, $multi);

                return 1;
            }
        };

        $adapter = new Mongo($client);
        $adapter->setAuthorization(new Authorization());
        $adapter->setNamespace('scope');
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant(self::TENANT);

        return $adapter;
    }
}
