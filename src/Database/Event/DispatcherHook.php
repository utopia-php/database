<?php

namespace Utopia\Database\Event;

use Exception;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Collection\Created as CollectionCreated;
use Utopia\Database\Event\Collection\Deleted as CollectionDeleted;
use Utopia\Database\Event\Document\Created as DocumentCreated;
use Utopia\Database\Event\Document\Deleted as DocumentDeleted;
use Utopia\Database\Event\Document\Updated as DocumentUpdated;
use Utopia\Database\Event\Documents\Created as DocumentsCreated;
use Utopia\Database\Event\Documents\Deleted as DocumentsDeleted;
use Utopia\Database\Event\Documents\Updated as DocumentsUpdated;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Selective;

/**
 * Delivers domain events to the listeners registered per event class and to an optional
 * PSR-14 dispatcher. Every listener and the dispatcher run; the first \Exception among them
 * is then rethrown, so the database applies its hook failure policy to it. An \Error is
 * never caught and reaches the caller at once.
 */
class DispatcherHook implements Lifecycle, Selective
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

    public function handles(Event $event): bool
    {
        $class = match ($event) {
            Event::DocumentCreate => DocumentCreated::class,
            Event::DocumentUpdate => DocumentUpdated::class,
            Event::DocumentDelete => DocumentDeleted::class,
            Event::DocumentsCreate => DocumentsCreated::class,
            Event::DocumentsUpdate => DocumentsUpdated::class,
            Event::DocumentsDelete => DocumentsDeleted::class,
            Event::CollectionCreate => CollectionCreated::class,
            Event::CollectionDelete => CollectionDeleted::class,
            default => null,
        };

        return $class !== null
            && (isset($this->listeners[$class]) || ($this->psr14Dispatcher !== null && \method_exists($this->psr14Dispatcher, 'dispatch')));
    }

    public function handle(Event $event, mixed $data): void
    {
        $domainEvent = $this->createDomainEvent($event, $data);

        if ($domainEvent === null) {
            return;
        }

        $failure = null;

        foreach ($this->listeners[$domainEvent::class] ?? [] as $listener) {
            try {
                $listener($domainEvent);
            } catch (Exception $exception) {
                $failure ??= $exception;
            }
        }

        if ($this->psr14Dispatcher !== null && \method_exists($this->psr14Dispatcher, 'dispatch')) {
            try {
                $this->psr14Dispatcher->dispatch($domainEvent);
            } catch (Exception $exception) {
                $failure ??= $exception;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    private function createDomainEvent(Event $event, mixed $data): ?Domain
    {
        return match ($event) {
            Event::DocumentCreate => $data instanceof Document
                ? new DocumentCreated($data->getCollection(), $data)
                : null,
            Event::DocumentUpdate => $data instanceof Document
                ? new DocumentUpdated($data->getCollection(), $data)
                : null,
            Event::DocumentDelete => $data instanceof Document
                ? new DocumentDeleted($data->getCollection(), $data->getId())
                : ($data instanceof \stdClass
                    && \is_string($data->collection ?? null)
                    && \is_string($data->id ?? null)
                    ? new DocumentDeleted($data->collection, $data->id)
                    : null),
            Event::DocumentsCreate => $this->createBulkEvent(DocumentsCreated::class, $data),
            Event::DocumentsUpdate => $this->createBulkEvent(DocumentsUpdated::class, $data),
            Event::DocumentsDelete => $this->createBulkEvent(DocumentsDeleted::class, $data),
            Event::CollectionCreate => $data instanceof Document
                ? new CollectionCreated($data->getId(), $data)
                : null,
            Event::CollectionDelete => match (true) {
                $data instanceof Document => new CollectionDeleted($data->getId()),
                \is_string($data) => new CollectionDeleted($data),
                default => null,
            },
            default => null,
        };
    }

    /**
     * @param  class-string<DocumentsCreated|DocumentsUpdated|DocumentsDeleted>  $class
     */
    private function createBulkEvent(string $class, mixed $data): ?Domain
    {
        if (! $data instanceof Document) {
            return null;
        }

        $count = $data->getAttribute('modified');

        return \is_int($count) ? new $class($data->getCollection(), $count) : null;
    }
}
