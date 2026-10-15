<?php

namespace Utopia\Database\Event;

use Exception;
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Selective;

/**
 * Forwards typed events to the listeners registered per event class and to an optional PSR-14 dispatcher. Every
 * listener and the dispatcher run; the first \Exception among them is then rethrown, so the database applies its
 * hook failure policy to it. An \Error is never caught and reaches the caller at once.
 */
class DispatcherHook implements Lifecycle, Selective
{
    /** @var array<class-string<Domain>, list<callable>> */
    private array $listeners = [];

    public function __construct(
        private readonly ?object $dispatcher = null,
    ) {
    }

    /**
     * @template T of Domain
     *
     * @param  class-string<T>  $eventClass
     * @param  callable(T): void  $listener
     */
    public function on(string $eventClass, callable $listener): static
    {
        $this->listeners[$eventClass][] = $listener;

        return $this;
    }

    #[\Override]
    public function handles(Event $event): bool
    {
        $class = $event->domain();

        return $class !== null && (isset($this->listeners[$class]) || $this->dispatches());
    }

    #[\Override]
    public function handle(Domain $event): void
    {
        $failure = null;

        foreach ($this->listeners[$event::class] ?? [] as $listener) {
            try {
                $listener($event);
            } catch (Exception $exception) {
                $failure ??= $exception;
            }
        }

        if ($this->dispatcher !== null && \method_exists($this->dispatcher, 'dispatch')) {
            try {
                $this->dispatcher->dispatch($event);
            } catch (Exception $exception) {
                $failure ??= $exception;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    private function dispatches(): bool
    {
        return $this->dispatcher !== null && \method_exists($this->dispatcher, 'dispatch');
    }
}
