<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Event;

final class AttributeCreateTest extends TestCase
{
    public function testCreateAttributesFiresAttributeCreatePerAttributeThenAttributesCreate(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $database->createAttributes(HookFixture::COLLECTION, [
            Attribute::string(key: 'summary', size: 64),
            Attribute::integer(key: 'likes'),
        ]);

        $this->assertSame([
            Event::DocumentPurge,
            Event::AttributeCreate,
            Event::AttributeCreate,
            Event::AttributesCreate,
        ], $recorder->getEvents());

        $created = $recorder->getPayloads(Event::AttributeCreate);
        $this->assertSame(['posts/summary', 'posts/likes'], \array_map($this->describe(...), $created));

        $batches = $recorder->getPayloads(Event::AttributesCreate);
        $this->assertCount(1, $batches);
        $this->assertIsArray($batches[0]);
        $this->assertSame(['posts/summary', 'posts/likes'], \array_map($this->describe(...), $batches[0]));
    }

    public function testCreateAttributeFiresAttributeCreateOnce(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $database->createAttribute(HookFixture::COLLECTION, Attribute::string(key: 'summary', size: 64));

        $this->assertSame([Event::DocumentPurge, Event::AttributeCreate], $recorder->getEvents());
        $this->assertSame(['posts/summary'], \array_map($this->describe(...), $recorder->getPayloads(Event::AttributeCreate)));
    }

    private function describe(mixed $attribute): string
    {
        $this->assertInstanceOf(Document::class, $attribute);

        return $attribute->getCollection().'/'.$attribute->getId();
    }
}
