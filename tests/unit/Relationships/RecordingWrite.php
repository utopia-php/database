<?php

namespace Tests\Unit\Relationships;

use Utopia\Database\Document;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\WriteContext;

/**
 * Records every document creation and update the adapter reports to its write hooks, one entry per call.
 */
class RecordingWrite extends Interceptor
{
    /** @var list<array{string, string, list<string>}> */
    public array $writes = [];

    /**
     * @param  array<Document>  $documents
     */
    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
        $this->writes[] = ['create', $collection, \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents))];
    }

    public function afterDocumentUpdate(string $collection, string $id, Document $document, WriteContext $context): void
    {
        $this->writes[] = ['update', $collection, [$document->getId()]];
    }
}
