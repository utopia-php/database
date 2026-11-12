<?php

namespace Tests\Unit\Event;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Hook\Lifecycle;

class RecordingLifecycle implements Lifecycle
{
    /** @var list<Domain> */
    private array $events = [];

    #[\Override]
    public function handle(Domain $event): void
    {
        $this->events[] = $event;
    }

    /**
     * @return list<Event>
     */
    public function getEvents(): array
    {
        return \array_map(static fn (Domain $event): Event => $event->event, $this->events);
    }

    /**
     * The typed events recorded for $event, in the order they fired.
     *
     * @return list<Domain>
     */
    public function received(Event $event): array
    {
        return \array_values(\array_filter(
            $this->events,
            static fn (Domain $recorded): bool => $recorded->event === $event,
        ));
    }
}
