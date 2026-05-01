<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\CollectionCreated;
use Utopia\Database\Event\CollectionDeleted;
use Utopia\Database\Event\DocumentCreated;
use Utopia\Database\Event\DocumentDeleted;
use Utopia\Database\Event\DocumentUpdated;
use Utopia\Database\Event\DomainEvent;

class DomainEventTest extends TestCase
{
    public function testDomainEventConstructorWithCollectionAndEvent(): void
    {
        $event = new DomainEvent('users', Event::DocumentCreate);
        $this->assertEquals('users', $event->collection);
        $this->assertEquals(Event::DocumentCreate, $event->event);
    }

    public function testDomainEventOccurredAtAutoSetToNow(): void
    {
        $before = new \DateTimeImmutable();
        $event = new DomainEvent('users', Event::DocumentCreate);
        $after = new \DateTimeImmutable();

        $this->assertGreaterThanOrEqual($before, $event->occurredAt);
        $this->assertLessThanOrEqual($after, $event->occurredAt);
    }

    public function testDomainEventCustomOccurredAt(): void
    {
        $custom = new \DateTimeImmutable('2025-01-01 12:00:00');
        $event = new DomainEvent('users', Event::DocumentCreate, $custom);
        $this->assertSame($custom, $event->occurredAt);
    }

    public function testDocumentCreatedCarriesDocument(): void
    {
        $doc = new Document(['$id' => 'doc1', 'name' => 'Alice']);
        $event = new DocumentCreated('users', $doc);

        $this->assertSame($doc, $event->document);
        $this->assertEquals('users', $event->collection);
    }

    public function testDocumentCreatedHasCorrectEventType(): void
    {
        $doc = new Document(['$id' => 'doc1']);
        $event = new DocumentCreated('users', $doc);
        $this->assertEquals(Event::DocumentCreate, $event->event);
    }

    public function testDocumentUpdatedCarriesDocumentAndPrevious(): void
    {
        $doc = new Document(['$id' => 'doc1', 'name' => 'Bob']);
        $prev = new Document(['$id' => 'doc1', 'name' => 'Alice']);
        $event = new DocumentUpdated('users', $doc, $prev);

        $this->assertSame($doc, $event->document);
        $this->assertSame($prev, $event->previous);
        $this->assertEquals(Event::DocumentUpdate, $event->event);
    }

    public function testDocumentUpdatedWithNullPrevious(): void
    {
        $doc = new Document(['$id' => 'doc1']);
        $event = new DocumentUpdated('users', $doc);

        $this->assertSame($doc, $event->document);
        $this->assertNull($event->previous);
    }

    public function testDocumentDeletedCarriesDocumentId(): void
    {
        $event = new DocumentDeleted('users', 'doc-42');

        $this->assertEquals('doc-42', $event->documentId);
        $this->assertEquals('users', $event->collection);
        $this->assertEquals(Event::DocumentDelete, $event->event);
    }

    public function testCollectionCreatedCarriesDocument(): void
    {
        $doc = new Document(['$id' => 'col1', 'name' => 'users']);
        $event = new CollectionCreated('users', $doc);

        $this->assertSame($doc, $event->document);
        $this->assertEquals(Event::CollectionCreate, $event->event);
    }

    public function testCollectionDeletedHasCorrectEventType(): void
    {
        $event = new CollectionDeleted('users');
        $this->assertEquals(Event::CollectionDelete, $event->event);
        $this->assertEquals('users', $event->collection);
    }

    public function testDomainEventIsReadonly(): void
    {
        $event = new DomainEvent('users', Event::DocumentCreate);

        $this->assertInstanceOf(\DateTimeImmutable::class, $event->occurredAt);
        $this->assertEquals('users', $event->collection);
        $this->assertEquals(Event::DocumentCreate, $event->event);
    }

    public function testDocumentCreatedOccurredAtIsAutoPopulated(): void
    {
        $doc = new Document(['$id' => 'doc1']);
        $event = new DocumentCreated('users', $doc);

        $this->assertInstanceOf(\DateTimeImmutable::class, $event->occurredAt);
    }

    public function testDocumentDeletedOccurredAtIsAutoPopulated(): void
    {
        $event = new DocumentDeleted('users', 'doc1');
        $this->assertInstanceOf(\DateTimeImmutable::class, $event->occurredAt);
    }

    public function testCollectionDeletedOccurredAtIsAutoPopulated(): void
    {
        $event = new CollectionDeleted('users');
        $this->assertInstanceOf(\DateTimeImmutable::class, $event->occurredAt);
    }
}
