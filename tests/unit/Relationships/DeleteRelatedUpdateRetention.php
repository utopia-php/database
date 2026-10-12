<?php

namespace Tests\Unit\Relationships;

use Closure;
use Utopia\Database\Document;
use WeakReference;

/**
 * Watches the bulk updates written to one collection and records, before each write, how many
 * documents handed to earlier writes are still held somewhere.
 */
final class DeleteRelatedUpdateRetention
{
    public int $written = 0;

    public int $mostAlive = 0;

    /** @var list<WeakReference<Document>> */
    private array $earlier = [];

    public function __construct(private readonly string $collection)
    {
    }

    public function reset(): void
    {
        $this->written = 0;
        $this->mostAlive = 0;
        $this->earlier = [];
    }

    /**
     * @param  array<Document>  $documents
     * @param  Closure(): int  $write
     */
    public function watch(Document $collection, array $documents, Closure $write): int
    {
        if ($collection->getId() !== $this->collection) {
            return $write();
        }

        \gc_collect_cycles();
        $alive = 0;
        foreach ($this->earlier as $reference) {
            if ($reference->get() !== null) {
                $alive++;
            }
        }
        $this->mostAlive = \max($this->mostAlive, $alive);

        $modified = $write();

        foreach ($documents as $document) {
            $this->earlier[] = WeakReference::create($document);
            $this->written++;
        }

        return $modified;
    }
}
