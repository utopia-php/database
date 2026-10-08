<?php

namespace Tests\Unit\Adapter;

use Closure;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Timeout;
use Utopia\Database\Event;

final class SyncRecordingMemory extends Memory implements Feature\Connection, Feature\Schemaless, Feature\Timeouts
{
    use Timeout;

    /**
     * @var list<SyncSnapshot>
     */
    public array $snapshots = [];

    private bool $schemaless = false;

    /**
     * @var (Closure(self): void)|null
     */
    private ?Closure $mutation = null;

    /**
     * @param  Closure(self): void  $mutation
     */
    public function mutateOnNextPing(Closure $mutation): void
    {
        $this->mutation = $mutation;
    }

    #[\Override]
    public function ping(): bool
    {
        $this->snapshots[] = new SyncSnapshot(
            database: $this->getDatabase(),
            namespace: $this->getNamespace(),
            tenant: $this->getTenant(),
            metadata: $this->getMetadata(),
            transforms: $this->transforms,
            profiler: $this->getProfiler(),
            schemaless: $this->schemaless,
            timeouts: $this->timeouts,
        );

        $mutation = $this->mutation;
        $this->mutation = null;
        if ($mutation !== null) {
            $mutation($this);
        }

        return true;
    }

    public function last(): SyncSnapshot
    {
        $last = \end($this->snapshots);
        if ($last === false) {
            throw new \LogicException('No call reached the connection');
        }

        return $last;
    }

    #[\Override]
    public function reconnect(): void
    {
    }

    #[\Override]
    public function id(): string
    {
        return 'sync-recording';
    }

    #[\Override]
    public function hostname(): string
    {
        return '';
    }

    #[\Override]
    public function setSchemaless(bool $schemaless): static
    {
        $this->schemaless = $schemaless;

        return $this;
    }

    #[\Override]
    public function isSchemaless(): bool
    {
        return $this->schemaless;
    }

    #[\Override]
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        $this->setTimeoutState($milliseconds, $event);
    }

    #[\Override]
    public function clearTimeout(Event $event = Event::All): void
    {
        $this->clearTimeoutState($event);
    }
}
