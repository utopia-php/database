<?php

namespace Tests\Unit\Cache;

use Closure;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;

final class ObservedDatabase extends Database
{
    private ?Closure $validatorCallback = null;

    private int $validators = 0;

    public function observeValidators(Closure $callback): void
    {
        $this->validatorCallback = $callback;
        $this->validators = 0;
    }

    public function getObservedValidators(): int
    {
        return $this->validators;
    }

    #[\Override]
    protected function createDocumentsValidator(Document $collection): DocumentsValidator
    {
        if ($this->validatorCallback !== null) {
            $this->validators++;
            $callback = $this->validatorCallback;
            $this->validatorCallback = null;
            $callback();
        } elseif ($this->validators > 0) {
            $this->validators++;
        }

        return parent::createDocumentsValidator($collection);
    }
}
