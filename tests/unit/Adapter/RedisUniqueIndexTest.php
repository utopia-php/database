<?php

namespace Tests\Unit\Adapter;

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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

#[RequiresPhpExtension('redis')]
final class RedisUniqueIndexTest extends TestCase
{
    private const string USERS = 'users';

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

    /** @var list<mixed> */
    private array $queued = [];

    protected function setUp(): void
    {
        $this->authorization = new Authorization();
        $this->authorization->addRole(Role::any()->toString());
        $this->client = $this->fakeClient();
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

    private function database(): Database
    {
        return (new Database(new RedisAdapter($this->client), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase('redis_unique')
            ->setNamespace('redis_unique');
    }

    private function createUsers(Database $database): void
    {
        $database->createCollection(new Collection(
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
        $client->method('sMembers')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->members($key)));
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
