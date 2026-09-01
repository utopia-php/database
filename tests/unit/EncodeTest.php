<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;

final class EncodeTest extends TestCase
{
    public function testExplicitNullUsesDeclaredDefault(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $collection = new Collection(
            id: 'documents',
            attributes: [Attribute::string(key: 'status', default: 'pending')],
        );

        $encoded = $database->encode($collection, new Document(['status' => null]));

        $this->assertSame('pending', $encoded->getAttribute('status'));
    }
}
