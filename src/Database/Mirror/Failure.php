<?php

namespace Utopia\Database\Mirror;

use Throwable;
use Utopia\Database\Event;

/**
 * A change the destination of a Mirror failed to apply: the Database method that made it, the event of that change
 * when it has one, and the error the destination threw.
 */
final readonly class Failure
{
    public function __construct(
        public string $method,
        public ?Event $event,
        public Throwable $error,
    ) {
    }
}
