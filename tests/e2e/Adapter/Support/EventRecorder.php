<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Named;

/**
 * Records lifecycle events for the test to assert afterwards. It must never assert in
 * handle(): the database swallows a hook's exception at isolated events, which would
 * turn every failed assertion into a pass.
 */
final class EventRecorder implements Lifecycle, Named
{
    /** @var list<Event> */
    private array $events = [];

    private bool $recording = true;

    public function __construct(
        private readonly string $name,
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function handle(Event $event, mixed $data): void
    {
        if ($this->recording) {
            $this->events[] = $event;
        }
    }

    /**
     * Stop recording and return the events recorded so far.
     *
     * @return list<Event>
     */
    public function stop(): array
    {
        $this->recording = false;

        return $this->events;
    }
}
