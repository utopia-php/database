<?php

namespace Utopia\Database\Event;

use Utopia\Database\Event;

class DomainEvent
{
    public readonly \DateTimeImmutable $occurredAt;

    public function __construct(
        public readonly string $collection,
        public readonly Event $event,
        ?\DateTimeImmutable $occurredAt = null,
    ) {
        $this->occurredAt = $occurredAt ?? new \DateTimeImmutable();
    }
}
