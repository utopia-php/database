<?php

namespace Utopia\Database\Event;

use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

class EventDispatcherHook implements Lifecycle
{
    /** @var array<string, array<callable>> */
    private array $listeners = [];

    private ?object $psr14Dispatcher;

    public function __construct(?object $psr14Dispatcher = null)
    {
        $this->psr14Dispatcher = $psr14Dispatcher;
    }

    public function on(string $eventClass, callable $listener): void
    {
        $this->listeners[$eventClass][] = $listener;
    }

    public function handle(Event $event, mixed $data): void
    {
        $domainEvent = $this->createDomainEvent($event, $data);

        if ($domainEvent === null) {
            return;
        }

        $class = $domainEvent::class;
        foreach ($this->listeners[$class] ?? [] as $listener) {
            try {
                $listener($domainEvent);
            } catch (\Throwable) {
            }
        }

        if ($this->psr14Dispatcher !== null && \method_exists($this->psr14Dispatcher, 'dispatch')) {
            try {
                $this->psr14Dispatcher->dispatch($domainEvent);
            } catch (\Throwable) {
            }
        }
    }

    private function createDomainEvent(Event $event, mixed $data): ?DomainEvent
    {
        return match ($event) {
            Event::DocumentCreate, Event::DocumentsCreate => $data instanceof Document
                ? new DocumentCreated($data->getCollection(), $data)
                : null,
            Event::DocumentUpdate, Event::DocumentsUpdate => $data instanceof Document
                ? new DocumentUpdated($data->getCollection(), $data)
                : null,
            Event::DocumentDelete, Event::DocumentsDelete => $data instanceof Document
                ? new DocumentDeleted($data->getCollection(), $data->getId())
                : ($data instanceof \stdClass && isset($data->collection, $data->id)
                    ? new DocumentDeleted($data->collection, $data->id)
                    : null),
            Event::CollectionCreate => $data instanceof Document
                ? new CollectionCreated($data->getId(), $data)
                : null,
            Event::CollectionDelete => \is_string($data)
                ? new CollectionDeleted($data)
                : null,
            default => null,
        };
    }
}
