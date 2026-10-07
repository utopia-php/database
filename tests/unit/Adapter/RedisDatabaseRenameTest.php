<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Redis;
use RedisException;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Exception as DatabaseException;

#[RequiresPhpExtension('redis')]
final class RedisDatabaseRenameTest extends TestCase
{
    private const string DATABASES = 'utopia:ns:dbs';

    private const string DOCUMENT = 'utopia:ns:library:doc:books:hobbit';

    private const string GRANTS = 'utopia:ns:library:grants:books:r:any';

    private const string COLLECTIONS = 'utopia:ns:library:cols';

    /**
     * @var array<string, string|array<string, true>>
     */
    private array $keys = [];

    private ?string $failOn = null;

    protected function setUp(): void
    {
        $this->keys = [
            self::DATABASES => ['library' => true],
            self::DOCUMENT => '{"title":"The Hobbit"}',
            self::GRANTS => [self::DOCUMENT => true, 'elsewhere' => true],
            self::COLLECTIONS => ['books' => true],
        ];
    }

    public function testRenameMovesEveryKeyAndTheGrantMembers(): void
    {
        $this->assertTrue($this->adapter()->update('library', 'archive'));

        $this->assertSame([
            'utopia:ns:archive:cols' => ['books' => true],
            'utopia:ns:archive:doc:books:hobbit' => '{"title":"The Hobbit"}',
            'utopia:ns:archive:grants:books:r:any' => ['elsewhere' => true, 'utopia:ns:archive:doc:books:hobbit' => true],
            self::DATABASES => ['archive' => true],
        ], $this->sorted());
    }

    public function testAFailurePartWayMovesTheMovedKeysBack(): void
    {
        $before = $this->sorted();
        $this->failOn = self::COLLECTIONS;

        try {
            $this->adapter()->update('library', 'archive');
            $this->fail('A failed key move must fail the rename');
        } catch (DatabaseException $error) {
            $this->assertStringContainsString('connection lost', $error->getMessage());
        }

        $this->assertSame($before, $this->sorted());
    }

    public function testAKeyGonePartWayMovesTheMovedKeysBack(): void
    {
        $before = $this->sorted();
        $this->failOn = 'missing';

        try {
            $this->adapter()->update('library', 'archive');
            $this->fail('A key that cannot be moved must fail the rename');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to move '.self::COLLECTIONS.' to utopia:ns:archive:cols', $error->getMessage());
        }

        $this->assertSame($before, $this->sorted());
    }

    public function testSharedTablesRefuseTheRename(): void
    {
        $before = $this->sorted();
        $adapter = $this->adapter();
        $adapter->setSharedTables(true);

        try {
            $adapter->update('library', 'archive');
            $this->fail('A rename under shared tables must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot rename a database while shared tables are enabled', $error->getMessage());
        }

        $this->assertSame($before, $this->sorted());
    }

    private function adapter(): RedisAdapter
    {
        $adapter = new RedisAdapter($this->client());
        $adapter->setNamespace('ns');

        return $adapter;
    }

    private function client(): Redis
    {
        $client = self::createStub(Redis::class);
        $client->method('sIsMember')->willReturnCallback(fn (string $key, string $member): bool => \is_array($this->keys[$key] ?? null) && isset($this->keys[$key][$member]));
        $client->method('scan')->willReturnCallback(fn (mixed $iterator, ?string $pattern = null): array => \array_values(\array_filter(
            \array_keys($this->keys),
            static fn (string $key): bool => \fnmatch($pattern ?? '*', $key),
        )));
        $client->method('rename')->willReturnCallback(function (string $key, string $target): bool {
            if ($key === $this->failOn) {
                throw new RedisException('connection lost');
            }
            if ($this->failOn === 'missing' && $key === self::COLLECTIONS) {
                return false;
            }
            $this->keys[$target] = $this->keys[$key];
            unset($this->keys[$key]);

            return true;
        });
        $client->method('sMembers')->willReturnCallback(fn (string $key): array => \array_map(\strval(...), \array_keys((array) ($this->keys[$key] ?? []))));
        $client->method('del')->willReturnCallback(function (string $key): int {
            $existed = isset($this->keys[$key]);
            unset($this->keys[$key]);

            return (int) $existed;
        });
        $client->method('sAdd')->willReturnCallback(function (string $key, string ...$members): int {
            $set = \is_array($this->keys[$key] ?? null) ? $this->keys[$key] : [];
            foreach ($members as $member) {
                $set[$member] = true;
            }
            $this->keys[$key] = $set;

            return \count($members);
        });
        $client->method('sRem')->willReturnCallback(function (string $key, string ...$members): int {
            $set = \is_array($this->keys[$key] ?? null) ? $this->keys[$key] : [];
            foreach ($members as $member) {
                unset($set[$member]);
            }
            $this->keys[$key] = $set;

            return \count($members);
        });

        return $client;
    }

    /**
     * @return array<string, string|array<string, true>>
     */
    private function sorted(): array
    {
        $keys = $this->keys;
        foreach ($keys as &$value) {
            if (\is_array($value)) {
                \ksort($value);
            }
        }
        unset($value);
        \ksort($keys);

        return $keys;
    }
}
