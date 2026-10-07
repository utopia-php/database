<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * An adapter that talks to a server over a connection it can check, re-establish and identify.
 */
interface Connection
{
    public function ping(): bool;

    public function reconnect(): void;

    public function id(): string;

    public function hostname(): string;
}
