<?php

namespace Tests\Unit\Documents;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;

final class DocumentsValidatorDatabase extends Database
{
    public int $documentsValidators = 0;

    /**
     * @param  array<Document>  $joinedCollections
     */
    public function documentsValidator(Document $collection, array $joinedCollections = []): DocumentsValidator
    {
        return $this->getDocumentsValidator($collection, $joinedCollections);
    }

    /**
     * @param  array<Query>  $queries
     */
    public function queriesValidator(Document $collection, array $queries): Queries
    {
        return $this->getQueriesValidator($collection, $queries);
    }

    /**
     * @param  array<Document>  $joinedCollections
     */
    protected function getDocumentsValidator(Document $collection, array $joinedCollections = []): DocumentsValidator
    {
        $this->documentsValidators++;

        return parent::getDocumentsValidator($collection, $joinedCollections);
    }
}
