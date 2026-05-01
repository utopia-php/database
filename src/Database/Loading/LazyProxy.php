<?php

namespace Utopia\Database\Loading;

use Utopia\Database\Document;

class LazyProxy extends Document
{
    private bool $resolved = false;

    private ?Document $realDocument = null;

    private ?BatchLoader $batchLoader;

    private string $targetCollection;

    private string $targetId;

    public function __construct(BatchLoader $batchLoader, string $targetCollection, string $targetId)
    {
        parent::__construct(['$id' => $targetId]);
        $this->batchLoader = $batchLoader;
        $this->targetCollection = $targetCollection;
        $this->targetId = $targetId;
        $batchLoader->register($this, $targetCollection, $targetId);
    }

    public function resolveWith(?Document $document): void
    {
        $this->resolved = true;
        $this->realDocument = $document;

        if ($document !== null) {
            foreach ($document->getArrayCopy() as $key => $value) {
                parent::offsetSet($key, $value);
            }
        }
    }

    public function offsetGet(mixed $key): mixed
    {
        $this->ensureResolved();

        return parent::offsetGet($key);
    }

    public function offsetExists(mixed $key): bool
    {
        $this->ensureResolved();

        return parent::offsetExists($key);
    }

    public function getAttribute(string $name, mixed $default = null): mixed
    {
        $this->ensureResolved();

        return parent::getAttribute($name, $default);
    }

    public function getArrayCopy(array $allow = [], array $disallow = []): array
    {
        $this->ensureResolved();

        return parent::getArrayCopy($allow, $disallow);
    }

    public function isEmpty(): bool
    {
        $this->ensureResolved();

        return parent::isEmpty();
    }

    public function isResolved(): bool
    {
        return $this->resolved;
    }

    private function ensureResolved(): void
    {
        if ($this->resolved) {
            return;
        }

        $this->batchLoader?->resolve($this->targetCollection, $this->targetId);

        if (! $this->resolved) {
            $this->resolved = true;
        }
    }
}
