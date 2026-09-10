<?php

namespace Tests\E2E\Adapter\Support;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

final class EventRecorder implements Lifecycle
{
    /** @var list<Event> */
    private array $events;

    /** @param list<Event> $events */
    public function __construct(array $events, private readonly TestCase $test)
    {
        $this->events = $events;
    }

    public function handle(Event $event, mixed $data): void
    {
        $expected = \array_shift($this->events);

        $this->test->assertSame($expected, $event);
    }
}
