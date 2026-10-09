<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

#[RequiresPhpExtension('redis')]
final class RedisUniqueIndexTest extends TestCase
{
    private const string USERS = 'users';

    private const string NOTES = 'notes';

    private const string NOTE = 'note';

    private const string ALICE = 'alice';

    private const string BOB = 'bob';

    private const string CAROL = 'carol';

    private const string KEY_SEGMENT = 'doc';

    private const int TENANT = 5;

    private const int OTHER_TENANT = 6;

    private Authorization $authorization;

    private Redis $client;

    /** @var array<string, string> */
    private array $strings = [];

    /** @var array<string, array<string, true>> */
    private array $sets = [];

    /** @var array<string, array<string, string>> */
    private array $hashes = [];

    private bool $pipelining = false;

    private int $memberReads = 0;

    /** @var list<mixed> */
    private array $queued = [];

    #[\Override]
    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
        $this->client = $this->fakeClient();
    }

    public function testUpdateDocumentsRejectsADuplicateUniqueValue(): void
    {
        $database = $this->usersDatabase();

        try {
            $database->updateDocuments(self::USERS, new Document(['email' => 'first@example.test']), [Query::equal('$id', ['second'])]);
            $this->fail('A batch update onto another document\'s unique value must be rejected');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }

        $this->assertSame(['first@example.test', 'second@example.test', 'third@example.test'], $this->emails($database));
        $this->assertSame(1, $database->updateDocuments(self::USERS, new Document(['email' => 'second@example.test']), [Query::equal('$id', ['second'])]));
        $this->assertSame(1, $database->updateDocuments(self::USERS, new Document(['email' => 'renamed@example.test']), [Query::equal('$id', ['second'])]));
        $this->assertSame(['first@example.test', 'renamed@example.test', 'third@example.test'], $this->emails($database));
    }

    public function testUpsertRejectsADuplicateUniqueValue(): void
    {
        $database = $this->usersDatabase();

        try {
            $database->upsertDocuments(self::USERS, [new Document(['$id' => 'second', 'email' => 'first@example.test'])]);
            $this->fail('An upsert that updates onto another document\'s unique value must be rejected');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }

        $this->assertSame(['first@example.test', 'second@example.test', 'third@example.test'], $this->emails($database));
        $this->assertSame(1, $database->upsertDocuments(self::USERS, [new Document(['$id' => 'second', 'email' => 'renamed@example.test'])]));
        $this->assertSame(['first@example.test', 'renamed@example.test', 'third@example.test'], $this->emails($database));
    }

    public function testABatchCannotCollideWithItself(): void
    {
        $database = $this->usersDatabase();

        try {
            $database->updateDocuments(self::USERS, new Document(['email' => 'shared@example.test']), [Query::equal('$id', ['second', 'third'])]);
            $this->fail('A batch update that gives two documents one unique value must be rejected');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }

        try {
            $database->upsertDocuments(self::USERS, [
                new Document(['$id' => 'second', 'email' => 'shared@example.test']),
                new Document(['$id' => 'third', 'email' => 'shared@example.test']),
            ]);
            $this->fail('An upsert batch that gives two documents one unique value must be rejected');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }

        $this->assertSame(['first@example.test', 'second@example.test', 'third@example.test'], $this->emails($database));
    }

    public function testAnUpsertBatchReadsTheStoredDocumentsOnceForItsUniqueChecks(): void
    {
        $database = $this->usersDatabase();
        $this->memberReads = 0;
        $this->assertSame(1, $database->upsertDocuments(self::USERS, [new Document(['$id' => 'first', 'email' => 'first-moved@example.test'])]));
        $single = $this->memberReads;
        $this->memberReads = 0;

        $this->assertSame(4, $database->upsertDocuments(self::USERS, [
            new Document(['$id' => 'third', 'email' => 'moved@example.test']),
            new Document(['$id' => 'second', 'email' => 'renamed@example.test']),
            new Document(['$id' => 'fourth', 'email' => 'fourth@example.test']),
            new Document(['$id' => 'fifth', 'email' => 'fifth@example.test']),
        ]));

        $this->assertSame($single, $this->memberReads, 'The unique checks must read the collection once per batch, not once per document');
        $this->assertSame(['fifth@example.test', 'first-moved@example.test', 'fourth@example.test', 'moved@example.test', 'renamed@example.test'], $this->emails($database));
    }

    public function testAnUpsertBatchChecksEachDocumentAgainstTheOnesBeforeIt(): void
    {
        $database = $this->usersDatabase();

        try {
            $database->upsertDocuments(self::USERS, [
                new Document(['$id' => 'fourth', 'email' => 'second@example.test']),
                new Document(['$id' => 'second', 'email' => 'renamed@example.test']),
            ]);
            $this->fail('A new document that takes a unique value before the document holding it gives it up must be rejected');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }
        $this->assertSame(['first@example.test', 'second@example.test', 'third@example.test'], $this->emails($database));

        $this->assertSame(2, $database->upsertDocuments(self::USERS, [
            new Document(['$id' => 'second', 'email' => 'renamed@example.test']),
            new Document(['$id' => 'fourth', 'email' => 'second@example.test']),
        ]));
        $this->assertSame(['first@example.test', 'renamed@example.test', 'second@example.test', 'third@example.test'], $this->emails($database));
    }

    public function testTenantPerDocumentChecksTheDocumentsTenant(): void
    {
        $database = $this->database()
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true);
        $database->create();
        $this->createUsers($database);

        $database->createDocument(self::USERS, $this->user('first', 'taken@example.test')->setAttribute('$tenant', self::TENANT));

        try {
            $database->createDocument(self::USERS, $this->user('second', 'taken@example.test')->setAttribute('$tenant', self::TENANT));
            $this->fail('A duplicate under the document\'s own tenant must be rejected while another tenant is selected');
        } catch (UniqueException $exception) {
            $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        }

        $database->createDocument(self::USERS, $this->user('second', 'taken@example.test')->setAttribute('$tenant', self::OTHER_TENANT));

        $this->assertSame(['taken@example.test'], $database->withTenant(self::TENANT, fn (): array => $this->emails($database)));
        $this->assertSame(['taken@example.test'], $database->withTenant(self::OTHER_TENANT, fn (): array => $this->emails($database)));
    }

    /**
     * @return array<string, array{0: bool}>
     */
    public static function tenancies(): array
    {
        return [
            'dedicated tables' => [false],
            'shared tables' => [true],
        ];
    }

    #[DataProvider('tenancies')]
    public function testDroppingACollectionNamedLikeAKeySegmentKeepsOtherGrants(bool $sharedTables): void
    {
        $database = $this->notesDatabase($sharedTables);
        $database->createCollection(Collection::create(id: self::KEY_SEGMENT, attributes: [Attribute::string(key: 'title', size: 64)]));
        $database->createDocument(self::KEY_SEGMENT, new Document(['$id' => self::NOTE, '$permissions' => [Permission::read(Role::any())], 'title' => 'dropped']));

        $database->deleteCollection(self::KEY_SEGMENT);
        $database->updateDocument(self::NOTES, self::NOTE, $this->readers([self::ALICE]));

        $this->assertSame([self::NOTE], $this->readableBy($database, self::ALICE));
        $this->assertSame([], $this->readableBy($database, self::BOB), 'Dropping another collection must leave the grants a later revoke removes');
        $this->assertSame([], $this->keysOf($sharedTables, self::KEY_SEGMENT), 'Dropping a collection must remove every key it owns');
    }

    #[DataProvider('tenancies')]
    public function testDroppingACollectionRemovesGrantsWrittenBeforeTheRegistry(bool $sharedTables): void
    {
        $database = $this->notesDatabase($sharedTables);
        foreach ($this->keys('*:grants:*') as $registry) {
            $this->forget($registry);
        }

        $database->deleteCollection(self::NOTES);

        $this->assertSame([], $this->keysOf($sharedTables, self::NOTES), 'Dropping a collection must remove the grants written before the registry existed');
    }

    #[DataProvider('tenancies')]
    public function testDroppingACollectionRemovesRegisteredGrantsItsIdIndexMisses(bool $sharedTables): void
    {
        $database = $this->notesDatabase($sharedTables);
        foreach ($this->keys('*:idx:*'.self::NOTES) as $index) {
            $this->forget($index);
        }

        $database->deleteCollection(self::NOTES);

        $grants = \array_filter($this->keysOf($sharedTables, self::NOTES), static fn (string $key): bool => \str_contains($key, ':perm:'));
        $this->assertSame([], \array_values($grants), 'Dropping a collection must remove the grants it registered, even those its id index no longer lists');
    }

    #[DataProvider('tenancies')]
    public function testSizingACollectionNamedLikeAKeySegmentCountsOnlyItsOwnKeys(bool $sharedTables): void
    {
        $database = $this->notesDatabase($sharedTables);
        $database->createCollection(Collection::create(id: self::KEY_SEGMENT, attributes: [Attribute::string(key: 'title', size: 64)]));
        $database->createDocument(self::KEY_SEGMENT, new Document(['$id' => self::NOTE, '$permissions' => [Permission::read(Role::any())], 'title' => 'sized']));

        $this->assertSame($this->bytesOf($sharedTables, self::KEY_SEGMENT), $database->getSizeOfCollection(self::KEY_SEGMENT), 'A collection named like a key segment must not count other collections\' grants');
        $this->assertSame($this->bytesOf($sharedTables, self::NOTES), $database->getSizeOfCollection(self::NOTES));
    }

    #[DataProvider('tenancies')]
    public function testSizingCountsGrantsWrittenBeforeTheRegistry(bool $sharedTables): void
    {
        $database = $this->notesDatabase($sharedTables);
        foreach ($this->keys('*:grants:*') as $registry) {
            $this->forget($registry);
        }

        $this->assertSame($this->bytesOf($sharedTables, self::NOTES), $database->getSizeOfCollection(self::NOTES), 'Sizing must count the grants written before the registry existed');
    }

    #[DataProvider('tenancies')]
    public function testSizingCountsRegisteredGrantsItsIdIndexMisses(bool $sharedTables): void
    {
        $database = $this->notesDatabase($sharedTables);
        foreach ($this->keys('*:idx:*'.self::NOTES) as $index) {
            $this->forget($index);
        }
        $unindexed = $this->keys('*:redis_unique:doc:*'.self::NOTES.':'.self::NOTE);

        $this->assertCount(1, $unindexed);
        $expected = $this->bytesOf($sharedTables, self::NOTES) - $this->bytes($unindexed[0]);

        $this->assertSame($expected, $database->getSizeOfCollection(self::NOTES), 'Sizing must count the grants the collection registered, even those its id index no longer lists');
    }

    public function testSizingUnderSharedTablesCountsOnlyTheSelectedTenantsGrants(): void
    {
        $database = $this->notesDatabase(true);
        $expected = $this->bytesOf(true, self::NOTES);

        $database->withTenant(self::OTHER_TENANT, function () use ($database): void {
            $database->createCollection(Collection::create(
                id: self::NOTES,
                attributes: [Attribute::string(key: 'title', size: 64)],
                permissions: [Permission::create(Role::any())],
                documentSecurity: true,
            ));
            $database->createDocument(self::NOTES, $this->readers([self::CAROL])->setAttribute('$id', self::NOTE)->setAttribute('title', 'other tenant'));
        });

        $this->assertNotSame([], $this->keys('*:perm:t:'.self::OTHER_TENANT.':'.self::NOTES.':*'));
        $this->assertSame($expected, $database->getSizeOfCollection(self::NOTES), 'Sizing must count only the selected tenant\'s grants');
    }

    private function database(): Database
    {
        return (new Database(new RedisAdapter($this->client), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('redis_unique')
            ->setNamespace('redis_unique');
    }

    private function usersDatabase(): Database
    {
        $database = $this->database();
        $database->create();
        $this->createUsers($database);
        foreach (['first', 'second', 'third'] as $id) {
            $database->createDocument(self::USERS, $this->user($id, $id.'@example.test'));
        }

        return $database;
    }

    private function createUsers(Database $database): void
    {
        $database->createCollection(Collection::create(
            id: self::USERS,
            attributes: [Attribute::string(key: 'email', size: 128)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createIndex(self::USERS, Index::unique(key: 'emailUnique', attributes: ['email'], lengths: [128]));
    }

    private function user(string $id, string $email): Document
    {
        return new Document(['$id' => $id, 'email' => $email]);
    }

    private function notesDatabase(bool $sharedTables): Database
    {
        $database = $this->database()->setSharedTables($sharedTables);
        if ($sharedTables) {
            $database->setTenant(self::TENANT);
        }
        $database->create();
        $database->createCollection(Collection::create(
            id: self::NOTES,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: true,
        ));
        $database->createDocument(self::NOTES, $this->readers([self::ALICE, self::BOB])->setAttribute('$id', self::NOTE)->setAttribute('title', 'kept'));

        return $database;
    }

    /**
     * @param  list<string>  $readers
     */
    private function readers(array $readers): Document
    {
        return new Document([
            '$permissions' => \array_map(
                static fn (string $reader): string => Permission::read(Role::user($reader)),
                $readers,
            ),
        ]);
    }

    /**
     * @return list<string>
     */
    private function emails(Database $database): array
    {
        $emails = \array_map(
            static fn (Document $document): string => \is_string($email = $document->getAttribute('email')) ? $email : '',
            $database->find(self::USERS),
        );
        \sort($emails);

        return $emails;
    }

    /**
     * @return list<string>
     */
    private function readableBy(Database $database, string $reader): array
    {
        $roles = $this->authorization->getRoles();
        $this->authorization->cleanRoles();
        $this->authorization->addRole(Role::user($reader)->toString());

        try {
            return \array_values(\array_map(
                static fn (Document $document): string => $document->getId(),
                $database->find(self::NOTES),
            ));
        } finally {
            $this->authorization->cleanRoles();
            foreach ($roles as $role) {
                $this->authorization->addRole($role);
            }
        }
    }

    /**
     * @return list<string>
     */
    private function keysOf(bool $sharedTables, string $collection): array
    {
        $prefix = RedisAdapter::KEY_PREFIX.':redis_unique:redis_unique:';
        $tenant = $sharedTables ? 't:'.self::TENANT.':' : '';
        $owned = [
            $prefix.'meta:'.$collection,
            $prefix.'grants:'.$collection,
            $prefix.'idx:'.$tenant.$collection,
            $prefix.'seq:'.$tenant.$collection,
            $prefix.'doc:'.$tenant.$collection.':'.self::NOTE,
            $prefix.'perm:'.$tenant.'doc:'.$collection.':'.self::NOTE,
        ];
        foreach (['r', 'c', 'u', 'd'] as $letter) {
            foreach ([Role::any(), Role::user(self::ALICE), Role::user(self::BOB)] as $role) {
                $owned[] = $prefix.'perm:'.$tenant.$collection.':'.$letter.':'.$role->toString();
            }
        }

        return \array_values(\array_filter($owned, $this->has(...)));
    }

    private function bytesOf(bool $sharedTables, string $collection): int
    {
        $bytes = 0;
        foreach ($this->keysOf($sharedTables, $collection) as $key) {
            if (\str_contains($key, ':grants:') || \str_contains($key, ':seq:')) {
                continue;
            }
            $bytes += $this->bytes($key);
        }

        return $bytes;
    }

    private function bytes(string $key): int
    {
        if (isset($this->strings[$key])) {
            return \strlen($key) + \strlen($this->strings[$key]);
        }

        $bytes = \strlen($key);
        foreach ($this->hashes[$key] ?? [] as $field => $value) {
            $bytes += \strlen((string) $field) + \strlen($value);
        }
        foreach ($this->members($key) as $member) {
            $bytes += \strlen($member);
        }

        return $bytes;
    }

    private function fakeClient(): Redis
    {
        $client = self::createStub(Redis::class);
        $client->method('ping')->willReturn(true);
        $client->method('multi')->willReturnCallback(function () use ($client): Redis {
            $this->pipelining = true;
            $this->queued = [];

            return $client;
        });
        $client->method('exec')->willReturnCallback(function (): mixed {
            $replies = $this->queued;
            $this->pipelining = false;
            $this->queued = [];

            return $replies;
        });
        $client->method('discard')->willReturnCallback(function (): bool {
            $this->pipelining = false;
            $this->queued = [];

            return true;
        });
        $client->method('get')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->strings[$key] ?? false));
        $client->method('mGet')->willReturnCallback(fn (mixed $keys): mixed => $this->reply($client, \array_map(
            fn (mixed $key): string|false => $this->strings[$this->text($key)] ?? false,
            \is_array($keys) ? \array_values($keys) : [],
        )));
        $client->method('set')->willReturnCallback(function (string $key, mixed $value) use ($client): mixed {
            $this->forget($key);
            $this->strings[$key] = $this->text($value);

            return $this->reply($client, true);
        });
        $client->method('incr')->willReturnCallback(function (string $key, int $by = 1) use ($client): mixed {
            $value = (int) ($this->strings[$key] ?? 0) + $by;
            $this->strings[$key] = (string) $value;

            return $this->reply($client, $value);
        });
        $client->method('exists')->willReturnCallback(fn (mixed ...$keys): mixed => $this->reply(
            $client,
            \count(\array_filter($keys, fn (mixed $key): bool => $this->has($this->text($key)))),
        ));
        $client->method('del')->willReturnCallback(function (mixed $key, mixed ...$otherKeys) use ($client): mixed {
            $removed = 0;
            foreach ([...(\is_array($key) ? \array_values($key) : [$key]), ...$otherKeys] as $candidate) {
                $candidate = $this->text($candidate);
                $removed += (int) $this->has($candidate);
                $this->forget($candidate);
            }

            return $this->reply($client, $removed);
        });
        $client->method('sAdd')->willReturnCallback(function (string $key, mixed ...$members) use ($client): mixed {
            $added = 0;
            foreach ($members as $member) {
                $member = $this->text($member);
                $added += (int) ! isset($this->sets[$key][$member]);
                $this->sets[$key][$member] = true;
            }

            return $this->reply($client, $added);
        });
        $client->method('sRem')->willReturnCallback(function (string $key, mixed ...$members) use ($client): mixed {
            $removed = 0;
            foreach ($members as $member) {
                $member = $this->text($member);
                $removed += (int) isset($this->sets[$key][$member]);
                unset($this->sets[$key][$member]);
            }
            if (($this->sets[$key] ?? null) === []) {
                unset($this->sets[$key]);
            }

            return $this->reply($client, $removed);
        });
        $client->method('sMembers')->willReturnCallback(function (string $key) use ($client): mixed {
            $this->memberReads++;

            return $this->reply($client, $this->members($key));
        });
        $client->method('sIsMember')->willReturnCallback(fn (string $key, mixed $member): mixed => $this->reply($client, isset($this->sets[$key][$this->text($member)])));
        $client->method('sCard')->willReturnCallback(fn (string $key): mixed => $this->reply($client, \count($this->sets[$key] ?? [])));
        $client->method('sUnion')->willReturnCallback(fn (string ...$keys): mixed => $this->reply(
            $client,
            \array_values(\array_unique(\array_merge(...\array_map($this->members(...), $keys)))),
        ));
        $client->method('hSet')->willReturnCallback(function (string $key, string $field, mixed $value) use ($client): mixed {
            $added = (int) ! isset($this->hashes[$key][$field]);
            $this->hashes[$key][$field] = $this->text($value);

            return $this->reply($client, $added);
        });
        $client->method('hMSet')->willReturnCallback(function (string $key, mixed $fields) use ($client): mixed {
            foreach (\is_array($fields) ? $fields : [] as $field => $value) {
                $this->hashes[$key][(string) $field] = $this->text($value);
            }

            return $this->reply($client, true);
        });
        $client->method('hGet')->willReturnCallback(fn (string $key, string $field): mixed => $this->reply($client, $this->hashes[$key][$field] ?? false));
        $client->method('hGetAll')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->hashes[$key] ?? []));
        $client->method('hDel')->willReturnCallback(function (string $key, string ...$fields) use ($client): mixed {
            $removed = 0;
            foreach ($fields as $field) {
                $removed += (int) isset($this->hashes[$key][$field]);
                unset($this->hashes[$key][$field]);
            }
            if (($this->hashes[$key] ?? null) === []) {
                unset($this->hashes[$key]);
            }

            return $this->reply($client, $removed);
        });
        $client->method('rawCommand')->willThrowException(new \RedisException('MEMORY USAGE is not available'));
        $client->method('type')->willReturnCallback(fn (string $key): int => match (true) {
            isset($this->strings[$key]) => Redis::REDIS_STRING,
            isset($this->sets[$key]) => Redis::REDIS_SET,
            isset($this->hashes[$key]) => Redis::REDIS_HASH,
            default => Redis::REDIS_NOT_FOUND,
        });
        $client->method('scan')->willReturnCallback(fn (mixed $iterator, ?string $pattern = null): mixed => $this->keys($pattern ?? '*'));

        return $client;
    }

    private function reply(Redis $client, mixed $value): mixed
    {
        if (! $this->pipelining) {
            return $value;
        }
        $this->queued[] = $value;

        return $client;
    }

    private function text(mixed $value): string
    {
        return \is_scalar($value) ? (string) $value : '';
    }

    /**
     * @return list<string>
     */
    private function members(string $key): array
    {
        return \array_map($this->text(...), \array_keys($this->sets[$key] ?? []));
    }

    /**
     * @return list<string>
     */
    private function keys(string $pattern): array
    {
        $keys = \array_map($this->text(...), [...\array_keys($this->strings), ...\array_keys($this->sets), ...\array_keys($this->hashes)]);

        return \array_values(\array_filter($keys, static fn (string $key): bool => \fnmatch($pattern, $key)));
    }

    private function has(string $key): bool
    {
        return isset($this->strings[$key]) || isset($this->sets[$key]) || isset($this->hashes[$key]);
    }

    private function forget(string $key): void
    {
        unset($this->strings[$key], $this->sets[$key], $this->hashes[$key]);
    }
}
