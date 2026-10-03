<?php

namespace Tests\Unit\Event;

use Throwable;
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

final class FailingLifecycle implements Lifecycle
{
    public function __construct(
        private readonly Event $event,
        private readonly Throwable $failure,
    ) {
    }

    public function handle(Event $event, mixed $data): void
    {
        if ($event === $this->event) {
            throw $this->failure;
        }
    }
}
