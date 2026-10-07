<?php

namespace Tests\Unit\Mirror;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Mirror;

final class RelationshipsHookTest extends TestCase
{
    /**
     * @return iterable<string, array{bool}>
     */
    public static function preparing(): iterable
    {
        yield 'preparing' => [true];
        yield 'not preparing' => [false];
    }

    #[DataProvider('preparing')]
    public function testBothSidesKeepTheHookConfiguration(bool $prepare): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror($source, $destination);
        $hook = new Relationships($mirror, $prepare);

        $mirror->addHook($hook);

        $this->assertSame($hook, $mirror->getRelationshipHook());
        $this->assertSame($prepare, $source->getRelationshipHook()?->shouldPrepare());
        $this->assertSame($prepare, $destination->getRelationshipHook()?->shouldPrepare());
    }

    public function testTheSourceHookKeepsTheGivenConfigurationWithoutADestination(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror($source);

        $mirror->addHook(new Relationships($mirror, false));

        $this->assertFalse($source->getRelationshipHook()?->shouldPrepare());
    }
}
