<?php

namespace Tests\Unit\Event;

use Throwable;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Hook\Lifecycle;

final class FailingLifecycle implements Lifecycle
{
    public function __construct(
        private readonly Event $event,
        private readonly Throwable $failure,
    ) {
    }

    #[\Override]
    public function handle(Domain $event): void
    {
        if ($event->event === $this->event) {
            throw $this->failure;
        }
    }
}
