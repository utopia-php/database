<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

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

        $created = $recorder->received(Event::AttributeCreate);
        $this->assertSame(['posts/summary', 'posts/likes'], \array_map($this->describe(...), $created));

        $batches = $recorder->received(Event::AttributesCreate);
        $this->assertCount(1, $batches);
        $this->assertInstanceOf(Event\Attribute\BatchCreated::class, $batches[0]);
        $this->assertSame('posts', $batches[0]->collection);
        $this->assertSame(['summary', 'likes'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $batches[0]->attributes));
    }

    public function testCreateAttributeFiresAttributeCreateOnce(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $database->createAttribute(HookFixture::COLLECTION, Attribute::string(key: 'summary', size: 64));

        $this->assertSame([Event::DocumentPurge, Event::AttributeCreate], $recorder->getEvents());
        $this->assertSame(['posts/summary'], \array_map($this->describe(...), $recorder->received(Event::AttributeCreate)));
    }

    private function describe(Domain $event): string
    {
        $this->assertInstanceOf(Event\Attribute\Created::class, $event);

        return $event->collection.'/'.$event->attribute->key;
    }
}
