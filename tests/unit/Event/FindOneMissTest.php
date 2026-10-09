<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Query;

final class FindOneMissTest extends TestCase
{
    public function testAMissFiresNoFindEvent(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first']);
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $found = $database->findOne(HookFixture::COLLECTION, [Query::equal(Document::ID, ['missing'])]);

        $this->assertTrue($found->isEmpty());
        $this->assertSame([], $recorder->received(Event::DocumentFind));
    }

    public function testAMissOnAnEmptyCollectionFiresNoFindEvent(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $this->assertTrue($database->findOne(HookFixture::COLLECTION)->isEmpty());
        $this->assertSame([], $recorder->received(Event::DocumentFind));
    }

    public function testAHitFiresOneFindEventWithTheDocument(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second']);
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $found = $database->findOne(HookFixture::COLLECTION, [Query::equal(Document::ID, ['second'])]);

        $events = $recorder->received(Event::DocumentFind);
        $this->assertSame('second', $found->getId());
        $this->assertCount(1, $events);
        $this->assertInstanceOf(Event\Document\Found::class, $events[0]);
        $this->assertSame(HookFixture::COLLECTION, $events[0]->collection);
        $this->assertSame([$found], $events[0]->documents);
    }
}
