<?php

namespace Tests\Unit\Event;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Event;
use Utopia\Database\Query;

/**
 * aggregate() fires its own event, carrying the rows it returns, and never the find event, whose documents its
 * rows are not.
 */
final class AggregateEventTest extends TestCase
{
    public function testAggregateFiresTheAggregateEventWithItsRows(): void
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, ['first', 'second', 'third']);
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $rows = $database->aggregate(HookFixture::COLLECTION, [Query::count('*', 'total'), Query::sum('views', 'views')]);

        $this->assertSame([Event::DocumentAggregate], $recorder->getEvents());
        $aggregated = $recorder->received(Event::DocumentAggregate)[0];
        $this->assertInstanceOf(Event\Document\Aggregated::class, $aggregated);
        $this->assertSame(HookFixture::COLLECTION, $aggregated->collection);
        $this->assertSame([['total' => 3, 'views' => 6]], $aggregated->rows);
        $this->assertSame($rows, $aggregated->rows);
    }

    public function testAnUnheardAggregateFiresNothing(): void
    {
        $database = HookFixture::sqlite();
        HookFixture::seed($database, ['first']);
        $recorder = new RecordingLifecycle();
        $database->addHook($recorder);

        $database->silent(fn (): array => $database->aggregate(HookFixture::COLLECTION, [Query::count()]));

        $this->assertSame([], $recorder->getEvents());
    }
}
