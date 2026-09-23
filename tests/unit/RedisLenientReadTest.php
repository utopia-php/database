<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

final class RedisLenientReadTest extends TestCase
{
    private const string COLLECTION = 'notes';

    public function testGetDocumentDropsAStoredNonStringPermission(): void
    {
        $client = self::createStub(Redis::class);
        $client->method('get')->willReturn(self::payload('note'));

        $document = self::adapter($client)->getDocument(new Document([Document::ID => self::COLLECTION]), 'note');

        $this->assertSame('note', $document->getId());
        $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
    }

    public function testFindDropsAStoredNonStringPermission(): void
    {
        $client = self::createStub(Redis::class);
        $client->method('exists')->willReturn(1);
        $client->method('sMembers')->willReturn(['first', 'second']);
        $client->method('mGet')->willReturn([self::payload('first'), self::payload('second')]);

        $authorization = new Authorization();
        $authorization->disable();

        $documents = self::adapter($client, $authorization)->find(new Document([Document::ID => self::COLLECTION]));

        $this->assertSame(['first', 'second'], \array_map(fn (Document $document): string => $document->getId(), $documents));
        foreach ($documents as $document) {
            $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
        }
    }

    private static function payload(string $id): string
    {
        return \json_encode([
            Document::ID => $id,
            Document::PERMISSIONS => [Permission::read(Role::any()), 42, null, Permission::read(Role::any())],
            'title' => 'stored',
        ], JSON_THROW_ON_ERROR);
    }

    private static function adapter(Redis $client, Authorization $authorization = new Authorization()): RedisAdapter
    {
        $adapter = new RedisAdapter($client);
        $adapter->setAuthorization($authorization);
        $adapter->setNamespace('lenient');

        return $adapter;
    }
}
