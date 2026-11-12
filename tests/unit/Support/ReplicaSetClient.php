<?php

namespace Tests\Unit\Support;

use Utopia\Mongo\Client;

/**
 * A MongoDB client that needs no server, a replica set unless told otherwise, and tallies the sessions and
 * transactions an adapter runs.
 */
final class ReplicaSetClient extends Client
{
    public int $sessions = 0;

    public int $commits = 0;

    public int $aborts = 0;

    public function __construct(private readonly bool $replicaSet = true)
    {
    }

    #[\Override]
    public function connect(): self
    {
        return $this;
    }

    #[\Override]
    public function close(): void
    {
    }

    #[\Override]
    public function isReplicaSet(): bool
    {
        return $this->replicaSet;
    }

    /**
     * @param  array<mixed>  $options
     * @return array<mixed>
     */
    #[\Override]
    public function startSession(array $options = []): array
    {
        $this->sessions++;

        return ['id' => (object) ['id' => $this->sessions]];
    }

    /**
     * @param  array<mixed>  $session
     * @param  array<mixed>  $options
     */
    #[\Override]
    public function startTransaction(array $session, array $options = []): bool
    {
        return true;
    }

    /**
     * @param  array<mixed>  $session
     * @param  array<mixed>  $options
     */
    #[\Override]
    public function commitTransaction(array $session, array $options = []): bool
    {
        $this->commits++;

        return true;
    }

    /**
     * @param  array<mixed>  $session
     * @param  array<mixed>  $options
     */
    #[\Override]
    public function abortTransaction(array $session, array $options = []): bool
    {
        $this->aborts++;

        return true;
    }

    /**
     * @param  array<mixed>  $sessions
     * @param  array<mixed>  $options
     */
    #[\Override]
    public function endSessions(array $sessions, array $options = []): bool
    {
        return true;
    }
}
