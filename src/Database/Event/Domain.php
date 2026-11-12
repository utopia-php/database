<?php

namespace Utopia\Database\Event;

use Utopia\Database\Event;

/**
 * A lifecycle event. Each {@see Event} case has one final subclass carrying the typed payload of that event; it is
 * built only when a registered lifecycle hook handles the event.
 */
abstract readonly class Domain
{
    public function __construct(
        public Event $event,
    ) {
    }
}
