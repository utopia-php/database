<?php

namespace Tests\E2E\Adapter\Support;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Decorator;
use Utopia\Database\Hook\Lifecycle;

final class MutationRecorder implements Decorator, Lifecycle
{
    public bool $armed = false;

    public int $decorations = 0;

    public int $events = 0;

    public function decorate(Event $event, Document $collection, Document $document): Document
    {
        if ($this->armed && \in_array($event, [Event::DocumentUpdate, Event::DocumentDelete], true)) {
            $this->decorations++;
        }

        return $document;
    }

    public function handle(Event $event, mixed $data): void
    {
        if ($this->armed && \in_array($event, [Event::DocumentUpdate, Event::DocumentDelete], true)) {
            $this->events++;
        }
    }
}
