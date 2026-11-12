<?php

namespace Tests\Unit\Documents;

use Utopia\Database\Document;
use Utopia\Database\Mirror;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;

final class DocumentsValidatorMirror extends Mirror
{
    /**
     * @param  array<Document>  $joinedCollections
     */
    public function documentsValidator(Document $collection, array $joinedCollections = []): DocumentsValidator
    {
        return $this->getDocumentsValidator($collection, $joinedCollections);
    }
}
