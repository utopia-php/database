<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Document;

/**
 * Under ignoreDuplicates() Redis returns only the documents it inserted, so the database counts
 * and emits the same documents on every adapter.
 */
final class RedisSkipDuplicatesTest extends TestCase
{
    private const string STORED = 'stored';

    public function testOnlyDocumentsWithANewIdAreReturned(): void
    {
        $client = self::createStub(Redis::class);
        $client->method('exists')->willReturnCallback(static fn (mixed $key): int => \is_string($key) && \str_contains($key, self::STORED) ? 1 : 0);
        $client->method('get')->willReturn(\json_encode([Document::ID => self::STORED, Document::SEQUENCE => '7'], JSON_THROW_ON_ERROR));
        $client->method('incr')->willReturn(8);

        $adapter = new RedisAdapter($client);
        $adapter->setNamespace('skip_duplicates');

        $created = $adapter->ignoreDuplicates(fn (): array => $adapter->createDocuments(new Document([Document::ID => 'notes']), [
            new Document([Document::ID => self::STORED]),
            new Document([Document::ID => 'fresh']),
        ]));

        $this->assertSame(['fresh'], \array_map(static fn (Document $document): string => $document->getId(), $created));
        $this->assertSame('8', $created[0]->getSequence());
    }

    public function testASingleSkippedDocumentStillCarriesTheStoredSequence(): void
    {
        $client = self::createStub(Redis::class);
        $client->method('exists')->willReturn(1);
        $client->method('get')->willReturn(\json_encode([Document::ID => self::STORED, Document::SEQUENCE => '7'], JSON_THROW_ON_ERROR));

        $adapter = new RedisAdapter($client);
        $adapter->setNamespace('skip_duplicates');

        $document = $adapter->ignoreDuplicates(fn (): Document => $adapter->createDocument(
            new Document([Document::ID => 'notes']),
            new Document([Document::ID => self::STORED]),
        ));

        $this->assertSame('7', $document->getSequence());
    }
}
