<?php

namespace Utopia\Database\Mirroring;

use Utopia\Database\Database;
use Utopia\Database\Document;

abstract class Filter
{
    abstract public function onCreateDocument(
        Database $source,
        Database $destination,
        string $collection,
        Document $document,
    ): Document;

    abstract public function onUpdateDocument(
        Database $source,
        Database $destination,
        string $collection,
        Document $document,
    ): Document;

    abstract public function onDeleteDocument(
        Database $source,
        Database $destination,
        string $collection,
    ): void;
}
