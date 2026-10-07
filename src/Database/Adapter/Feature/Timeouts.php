<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Event;

/**
 * An adapter that bounds how long its statements may run, globally or for one event.
 */
interface Timeouts
{
    public function setTimeout(int $milliseconds, Event $event = Event::All): void;

    public function clearTimeout(Event $event = Event::All): void;

    /**
     * The bound statements of the event run under: its own, else the global one, else 0 for none.
     */
    public function getTimeout(Event $event = Event::All): int;
}
