<?php

namespace Tests\E2E\Adapter\Support;

use UnexpectedValueException;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Named;

/**
 * Records lifecycle events for the test to assert afterwards. It must never assert in
 * handle(): the database swallows a hook's exception at isolated events, which would
 * turn every failed assertion into a pass.
 */
final class EventRecorder implements Lifecycle, Named
{
    /** @var list<Domain> */
    private array $events = [];

    private bool $recording = true;

    public function __construct(
        private readonly string $name,
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function handle(Domain $event): void
    {
        if ($this->recording) {
            $this->events[] = $event;
        }
    }

    /**
     * Stop recording and return the events recorded so far.
     *
     * @return list<Event>
     */
    public function stop(): array
    {
        $this->recording = false;

        return \array_map(static fn (Domain $event): Event => $event->event, $this->events);
    }

    /**
     * The typed events recorded for $event so far, in the order they fired.
     *
     * @return list<Domain>
     */
    public function received(Event $event): array
    {
        return \array_values(\array_filter(
            $this->events,
            static fn (Domain $recorded): bool => $recorded->event === $event,
        ));
    }

    /**
     * The documents the events recorded for $event so far carry, in the order they fired.
     *
     * @return list<Document>
     *
     * @throws UnexpectedValueException When an event recorded for $event carries no document
     */
    public function getDocuments(Event $event): array
    {
        $documents = [];
        foreach ($this->received($event) as $recorded) {
            $document = \property_exists($recorded, 'document') ? $recorded->document : null;
            if (! $document instanceof Document) {
                throw new UnexpectedValueException($event->value.' recorded a '.$recorded::class.', which carries no document');
            }
            $documents[] = $document;
        }

        return $documents;
    }
}
