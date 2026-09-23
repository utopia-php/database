<?php

namespace Tests\Unit\Event;

use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

class RecordingLifecycle implements Lifecycle
{
    /** @var list<Event> */
    private array $events = [];

    /** @var list<mixed> */
    private array $payloads = [];

    public function handle(Event $event, mixed $data): void
    {
        $this->events[] = $event;
        $this->payloads[] = $data;
    }

    /**
     * @return list<Event>
     */
    public function getEvents(): array
    {
        return $this->events;
    }

    /**
     * @return list<mixed>
     */
    public function getPayloads(Event $event): array
    {
        $payloads = [];
        foreach ($this->events as $index => $recorded) {
            if ($recorded === $event) {
                $payloads[] = $this->payloads[$index];
            }
        }

        return $payloads;
    }
}
