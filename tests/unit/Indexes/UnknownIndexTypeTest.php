<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Index;

final class UnknownIndexTypeTest extends TestCase
{
    public function testAnUnknownTypeIsRefusedAs7xDid(): void
    {
        $this->expectException(IndexException::class);
        $this->expectExceptionMessage('Unknown index type: nope. Must be one of key, unique, fulltext, spatial, object, hnsw_euclidean, hnsw_cosine, hnsw_dot, trigram, ttl');

        Index::fromArray(['key' => 'i', 'type' => 'nope', 'attributes' => ['name']]);
    }

    public function testAMissingTypeIsAKeyIndex(): void
    {
        $this->assertSame('key', Index::fromArray(['key' => 'i', 'attributes' => ['name']])->type->value);
    }
}
