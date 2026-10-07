<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Tests\Unit\Event\RecordingLifecycle;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Mirror;
use Utopia\Query\Hook;

final class AddUnknownHookTest extends TestCase
{
    public function testAnUnknownHookIsRefused(): void
    {
        $database = HookFixture::memory();
        $hook = new class () implements Hook {};

        try {
            $database->addHook($hook);
            $this->fail('An unknown hook was accepted');
        } catch (DatabaseException $error) {
            $this->assertSame('Unknown hook: '.$hook::class, $error->getMessage());
        }
    }

    public function testAnUnknownHookIsRefusedThroughAMirror(): void
    {
        $mirror = new Mirror(HookFixture::memory(), new Database(new Memory(), new Cache(new None())));

        $this->expectException(DatabaseException::class);

        $mirror->addHook(new class () implements Hook {});
    }

    public function testAKnownHookIsRegistered(): void
    {
        $database = HookFixture::memory();
        $recorder = new RecordingLifecycle();

        $this->assertSame($database, $database->addHook($recorder));

        $database->getCollection(HookFixture::COLLECTION);
        $this->assertSame([Event::CollectionRead], $recorder->getEvents());
    }
}
