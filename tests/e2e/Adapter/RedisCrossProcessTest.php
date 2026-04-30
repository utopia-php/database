<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\Redis as RedisCacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisDbAdapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Verifies that two PHP processes sharing the same Redis backend see
 * the same database state through the Redis adapter — the entire point
 * of the adapter. The parent writes a document, a child process reads
 * and mutates it, and the parent then observes the child's mutation.
 *
 * Skips if `proc_open` is disabled — there is no in-process fallback,
 * since the cross-process behaviour is the property under test.
 */
class RedisCrossProcessTest extends TestCase
{
    private const HELPER_SCRIPT = __DIR__ . '/_helpers/redis_cross_process_worker.php';

    protected ?Authorization $authorization = null;
    protected ?Redis $redisClient = null;
    protected string $namespace = '';

    public function setUp(): void
    {
        parent::setUp();

        $disabled = \explode(',', \ini_get('disable_functions') ?: '');
        $disabled = \array_map('trim', $disabled);
        if (\in_array('proc_open', $disabled, true)) {
            $this->markTestIncomplete('proc_open required — cross-process Redis adapter test cannot run.');
        }

        $this->authorization = new Authorization();
        $this->authorization->addRole('any');
    }

    public function tearDown(): void
    {
        try {
            if ($this->namespace !== '' && $this->redisClient instanceof Redis) {
                $client = $this->redisClient;
                $iterator = null;
                // Adapter keys are prefixed with `KEY_PREFIX:` — without the
                // prefix this SCAN matches nothing and leaks every key.
                $pattern = RedisDbAdapter::KEY_PREFIX . ':' . $this->namespace . ':*';
                while (($keys = $client->scan($iterator, $pattern, 500)) !== false) {
                    if (\is_array($keys) && \count($keys) > 0) {
                        $client->del($keys);
                    }
                    if ($iterator === 0) {
                        break;
                    }
                }
            }
        } finally {
            $this->namespace = '';
            $this->redisClient = null;
            parent::tearDown();
        }
    }

    public function testCrossProcessReadWrite(): void
    {
        $host = \getenv('REDIS_HOST') ?: 'redis-mirror';
        $port = (int) (\getenv('REDIS_PORT') ?: 6379);

        $redis = new Redis();
        $redis->connect($host, $port);
        $this->redisClient = $redis;

        $this->namespace = 'utopia_xp_' . \uniqid();

        $cacheHost = \getenv('CACHE_REDIS_HOST') ?: 'redis';
        $cachePort = (int) (\getenv('CACHE_REDIS_PORT') ?: 6379);
        $cacheRedis = new Redis();
        $cacheRedis->connect($cacheHost, $cachePort);
        $cache = new Cache(new RedisCacheAdapter($cacheRedis));

        $database = new Database(new RedisDbAdapter($redis), $cache);
        $database
            ->setAuthorization($this->authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace($this->namespace);

        $database->create();

        $database->createCollection('crossproc', [
            new Document([
                '$id' => 'value',
                'type' => Database::VAR_STRING,
                'size' => 64,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ]);

        $documentId = 'xp-1';
        $database->createDocument('crossproc', new Document([
            '$id' => $documentId,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'value' => 'hello',
        ]));

        $command = [
            \PHP_BINARY,
            self::HELPER_SCRIPT,
            $this->namespace,
            $documentId,
            'read-and-update',
        ];

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $env = [
            'REDIS_HOST' => $host,
            'REDIS_PORT' => (string) $port,
            'CACHE_REDIS_HOST' => $cacheHost,
            'CACHE_REDIS_PORT' => (string) $cachePort,
            'PATH' => \getenv('PATH') ?: '/usr/local/bin:/usr/bin:/bin',
        ];

        $process = \proc_open($command, $descriptors, $pipes, null, $env);
        if (! \is_resource($process)) {
            $this->fail('Failed to spawn child PHP process via proc_open.');
        }

        \fclose($pipes[0]);
        $stdout = \stream_get_contents($pipes[1]) ?: '';
        $stderr = \stream_get_contents($pipes[2]) ?: '';
        \fclose($pipes[1]);
        \fclose($pipes[2]);

        $exitCode = \proc_close($process);

        if ($exitCode !== 0) {
            $this->fail(
                "Child process exited with status {$exitCode}.\n" .
                "STDOUT:\n{$stdout}\n" .
                "STDERR:\n{$stderr}"
            );
        }

        $this->assertStringContainsString('OK', $stdout);

        $reread = $database->getDocument('crossproc', $documentId);
        $this->assertFalse($reread->isEmpty(), 'Document disappeared after child update.');
        $this->assertSame(
            'world',
            $reread->getAttribute('value'),
            'Parent did not observe the child process update — Redis adapter state is not actually shared.'
        );
    }
}
