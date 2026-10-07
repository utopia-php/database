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
        $this->assertSame([], $recorder->getPayloads(Event::DocumentFind));
    }

    public function testAMissOnAnEmptyCollectionFiresNoFindEvent(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $this->assertTrue($database->findOne(HookFixture::COLLECTION)->isEmpty());
        $this->assertSame([], $recorder->getPayloads(Event::DocumentFind));
    }

    public function testAHitFiresOneFindEventWithTheDocument(): void
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second']);
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $found = $database->findOne(HookFixture::COLLECTION, [Query::equal(Document::ID, ['second'])]);

        $payloads = $recorder->getPayloads(Event::DocumentFind);
        $this->assertSame('second', $found->getId());
        $this->assertCount(1, $payloads);
        $this->assertInstanceOf(Document::class, $payloads[0]);
        $this->assertSame('second', $payloads[0]->getId());
    }
}
