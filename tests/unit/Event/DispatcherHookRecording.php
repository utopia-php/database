<?php

namespace Tests\Unit\Event;

use Utopia\Database\Event\Domain;

final class DispatcherHookRecording
{
    /** @var list<Domain> */
    public array $events = [];

    public function add(Domain $event): void
    {
        $this->events[] = $event;
    }

    /**
     * @return list<array{class-string<Domain>, string}>
     */
    public function summary(): array
    {
        return \array_map(
            static fn (Domain $event): array => [$event::class, $event->collection],
            $this->events,
        );
    }
}
