<?php

namespace Tests\Unit\Hook;

use RuntimeException;
use Utopia\Database\Database;
use Utopia\Database\Hook\Attachable;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\WriteContext;

/**
 * A write hook for {@see AttachTest} that records the databases it is attached to and the documents it sees created.
 */
final class AttachTestHook extends Interceptor implements Attachable
{
    /** @var list<Database> */
    public array $attached = [];

    /** @var list<string> */
    public array $created = [];

    public function __construct(
        private readonly ?RuntimeException $failure = null,
    ) {
    }

    public function attach(Database $database): void
    {
        if ($this->failure !== null) {
            throw $this->failure;
        }

        $this->attached[] = $database;
    }

    public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
    {
        foreach ($documents as $document) {
            $this->created[] = $document->getId();
        }
    }
}
