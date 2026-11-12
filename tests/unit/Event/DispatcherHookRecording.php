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
     * @return list<class-string<Domain>>
     */
    public function classes(): array
    {
        return \array_map(static fn (Domain $event): string => $event::class, $this->events);
    }
}
