<?php

namespace Tests\Unit\Documents;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;

final class DocumentsValidatorDatabase extends Database
{
    /**
     * @param  array<Document>  $joinedCollections
     */
    public function documentsValidator(Document $collection, array $joinedCollections = []): DocumentsValidator
    {
        return $this->getDocumentsValidator($collection, $joinedCollections);
    }
}
