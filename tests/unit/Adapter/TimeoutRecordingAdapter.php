<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Event;

final class TimeoutRecordingAdapter extends Memory implements Feature\Timeouts
{
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        $this->setTimeoutState($milliseconds, $event);
    }

    public function clearTimeout(Event $event = Event::All): void
    {
        $this->clearTimeoutState($event);
    }
}
