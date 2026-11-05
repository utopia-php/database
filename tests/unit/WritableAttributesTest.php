<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Document;
use Utopia\Database\Storage;

final class WritableAttributesTest extends TestCase
{
    public function testStorageColumnKeysAreDropped(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @return array<string, mixed>
             */
            public function writable(Document $document): array
            {
                return $this->writableAttributes($document);
            }
        };

        $document = new Document([
            '$id' => 'kept',
            '$permissions' => [],
            'name' => 'kept',
            '_private' => 'kept',
            Storage::UID => 'dropped',
            '_u!id' => 'dropped',
            Storage::SEQUENCE => 1,
            Storage::TENANT => 2,
            '_ten.ant' => 2,
            Storage::PERMISSIONS => '[]',
            Storage::CREATED_AT => 'dropped',
            Storage::UPDATED_AT => 'dropped',
        ]);

        $this->assertSame(['name' => 'kept', '_private' => 'kept'], $adapter->writable($document));
    }
}
