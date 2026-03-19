<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\DocumentCreated;
use Utopia\Database\Event\DocumentDeleted;
use Utopia\Database\Event\DocumentUpdated;
use Utopia\Database\Event\EventDispatcherHook;

class EventDispatcherHookTest extends TestCase
{
    private EventDispatcherHook $hook;

    protected function setUp(): void
    {
        $this->hook = new EventDispatcherHook();
    }

    public function testDocumentCreatedEvent(): void
    {
        $received = null;
        $this->hook->on(DocumentCreated::class, function (DocumentCreated $event) use (&$received) {
            $received = $event;
        });

        $doc = new Document([
            '$id' => 'doc-1',
            '$collection' => 'users',
        ]);

        $this->hook->handle(Event::DocumentCreate, $doc);

        $this->assertInstanceOf(DocumentCreated::class, $received);
        $this->assertEquals('users', $received->collection);
        $this->assertSame($doc, $received->document);
    }

    public function testDocumentUpdatedEvent(): void
    {
        $received = null;
        $this->hook->on(DocumentUpdated::class, function (DocumentUpdated $event) use (&$received) {
            $received = $event;
        });

        $doc = new Document([
            '$id' => 'doc-2',
            '$collection' => 'posts',
        ]);

        $this->hook->handle(Event::DocumentUpdate, $doc);

        $this->assertInstanceOf(DocumentUpdated::class, $received);
        $this->assertEquals('posts', $received->collection);
    }

    public function testDocumentDeletedEvent(): void
    {
        $received = null;
        $this->hook->on(DocumentDeleted::class, function (DocumentDeleted $event) use (&$received) {
            $received = $event;
        });

        $doc = new Document([
            '$id' => 'doc-3',
            '$collection' => 'users',
        ]);

        $this->hook->handle(Event::DocumentDelete, $doc);

        $this->assertInstanceOf(DocumentDeleted::class, $received);
        $this->assertEquals('doc-3', $received->documentId);
    }

    public function testUnhandledEventDoesNothing(): void
    {
        $called = false;
        $this->hook->on(DocumentCreated::class, function () use (&$called) {
            $called = true;
        });

        $this->hook->handle(Event::DatabaseCreate, 'test');

        $this->assertFalse($called);
    }

    public function testMultipleListeners(): void
    {
        $count = 0;
        $this->hook->on(DocumentCreated::class, function () use (&$count) {
            $count++;
        });
        $this->hook->on(DocumentCreated::class, function () use (&$count) {
            $count++;
        });

        $doc = new Document([
            '$id' => 'doc-4',
            '$collection' => 'test',
        ]);

        $this->hook->handle(Event::DocumentCreate, $doc);

        $this->assertEquals(2, $count);
    }

    public function testListenerExceptionDoesNotPropagate(): void
    {
        $secondCalled = false;

        $this->hook->on(DocumentCreated::class, function () {
            throw new \RuntimeException('boom');
        });
        $this->hook->on(DocumentCreated::class, function () use (&$secondCalled) {
            $secondCalled = true;
        });

        $doc = new Document([
            '$id' => 'doc-5',
            '$collection' => 'test',
        ]);

        $this->hook->handle(Event::DocumentCreate, $doc);

        $this->assertTrue($secondCalled);
    }
}
