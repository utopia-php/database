<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Event;

/**
 * Lets a {@see Lifecycle} hook name the events it handles. It receives only those, and the database skips work
 * whose only purpose is to feed events no registered hook handles.
 */
interface Selective
{
    public function handles(Event $event): bool;
}
