<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Event;

/**
 * The timeout state of an adapter that implements Feature\Timeouts.
 */
trait Timeout
{
    protected int $timeout = 0;

    /**
     * @var array<string, int>
     */
    protected array $timeouts = [];

    public function getTimeout(Event $event = Event::All): int
    {
        return $this->timeouts[$event->value]
            ?? $this->timeouts[Event::All->value]
            ?? $this->timeout;
    }

    protected function setTimeoutState(int $milliseconds, Event $event): void
    {
        $this->timeouts[$event->value] = $milliseconds;

        if ($event === Event::All) {
            $this->timeout = $milliseconds;
        }
    }

    protected function clearTimeoutState(Event $event): void
    {
        if ($event === Event::All) {
            $this->timeouts = [];
            $this->timeout = 0;

            return;
        }

        unset($this->timeouts[$event->value]);
    }
}
