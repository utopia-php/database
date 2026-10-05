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
use Utopia\Query\Schema\ColumnType;

final class EncodeTest extends TestCase
{
    use MagicAccessAssertions;

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

    public function testEncodeReadsAttributesWithoutMagicProperties(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $recorder = new MagicAccessRecorder();
        $recorder->start();
        $attributes = [
            CountingAttribute::of(new Attribute(key: 'status', default: 'pending'), $recorder),
            CountingAttribute::of(new Attribute(key: 'tags', type: ColumnType::Varchar, size: 32, array: true, default: ['new']), $recorder),
            CountingAttribute::of(new Attribute(key: 'title'), $recorder),
        ];

        $encoded = $database->encode(
            new Collection(id: 'documents', attributes: $attributes),
            new Document(['status' => null, 'title' => 'Demo']),
        );

        $this->assertSame('pending', $encoded->getAttribute('status'));
        $this->assertSame(['new'], $encoded->getAttribute('tags'));
        $this->assertSame('Demo', $encoded->getAttribute('title'));
        $this->assertNoMagicAccess($recorder, 'encode()');
    }
}
