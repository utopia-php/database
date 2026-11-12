<?php

namespace Utopia\Database\Adapter\Feature;

/**
 * An adapter that talks to a server over a connection it can check, re-establish and identify.
 */
interface Connection
{
    public function ping(): bool;

    public function reconnect(): void;

    /**
     * The connection's id. An engine with a server-side id (MariaDB, MySQL, Postgres) reports it; one without
     * (SQLite, MongoDB) reports its handle's object id, unique only within the process and only while the
     * handle lives, so it must not be compared across processes or stored. Redis reports no id ('0').
     */
    public function id(): string;

    public function hostname(): string;
}
