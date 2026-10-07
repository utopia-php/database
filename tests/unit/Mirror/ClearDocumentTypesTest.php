<?php

namespace Tests\Unit\Mirror;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Mirror;

final class ClearDocumentTypesTest extends TestCase
{
    public function testClearingEveryTypeKeepsTheMetadataCollectionHydratingToCollection(): void
    {
        $source = HookFixture::memory();
        $destination = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror($source, $destination);

        $mirror->clearAllDocumentTypes();

        $this->assertSame(Collection::class, $mirror->getDocumentType(Database::METADATA));
        $this->assertSame(Collection::class, $source->getDocumentType(Database::METADATA));
        $this->assertSame(Collection::class, $destination->getDocumentType(Database::METADATA));
        $this->assertInstanceOf(Collection::class, $mirror->getDocument(Database::METADATA, HookFixture::COLLECTION));
        $this->assertInstanceOf(Collection::class, $mirror->getCollection(HookFixture::COLLECTION));
    }

    public function testClearingEveryTypeDropsTheCustomTypes(): void
    {
        $source = HookFixture::memory();
        $mirror = new Mirror($source, new Database(new Memory(), new Cache(new None())));
        $mirror->setDocumentType(HookFixture::COLLECTION, ClearDocumentTypesPost::class);

        $mirror->clearAllDocumentTypes();

        $this->assertNull($mirror->getDocumentType(HookFixture::COLLECTION));
        $this->assertNull($source->getDocumentType(HookFixture::COLLECTION));
    }

    public function testSettingAnUnknownClassIsRefusedBeforeTheMirrorRecordsIt(): void
    {
        $mirror = new Mirror(HookFixture::memory());

        try {
            $mirror->setDocumentType(HookFixture::COLLECTION, 'Tests\Unit\Mirror\Missing');
            $this->fail('An unknown document class was accepted');
        } catch (DatabaseException $error) {
            $this->assertSame('Class Tests\Unit\Mirror\Missing does not exist', $error->getMessage());
        }

        $this->assertNull($mirror->getDocumentType(HookFixture::COLLECTION));
    }
}
