<?php

namespace Utopia\Database\Cache;

use Throwable;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

class Invalidator implements Lifecycle
{
    public function __construct(
        private QueryCache $queryCache,
        private Scope $scope = new Scope(),
    ) {
    }

    public function handle(Event $event, mixed $data): void
    {
        $this->invalidate($event, $data, $this->scope);
    }

    public function invalidate(Event $event, mixed $data, Scope $scope): void
    {
        $tokens = $this->tokens($event, $data, $scope);
        $this->block($tokens);
        $this->activate($tokens);
    }

    /**
     * With $tenantPerDocument, each document's collections are keyed under the tenant it is stored
     * under: its own, or the scope's when it has none.
     *
     * @return array<string, string> Tokens by the collection key they invalidate
     */
    public function tokens(Event $event, mixed $data, ?Scope $scope = null, bool $tenantPerDocument = false): array
    {
        if (! $this->isMutation($event)) {
            return [];
        }

        $scope ??= $this->scope;
        if (! $tenantPerDocument) {
            return $this->scopedTokens($event, $data, $scope);
        }

        $scopes = [];
        $targets = [];
        foreach (\is_array($data) ? $data : [$data] as $target) {
            $tenant = $target instanceof Document ? $target->getTenant() ?? $scope->tenant : $scope->tenant;
            $key = \serialize($tenant);
            $scopes[$key] ??= new Scope(
                hostname: $scope->hostname,
                database: $scope->database,
                namespace: $scope->namespace,
                tenant: $tenant,
            );
            $targets[$key][] = $target;
        }

        $tokens = [];
        foreach ($scopes as $key => $tenantScope) {
            $tokens += $this->scopedTokens($event, $targets[$key], $tenantScope);
        }

        return $tokens;
    }

    /**
     * @param  array<string, string>  $tokens
     */
    public function block(array $tokens): void
    {
        foreach ($tokens as $key => $token) {
            $this->queryCache->blockCollection($key, $token);
        }
    }

    /**
     * @param  array<string, string>  $tokens
     */
    public function activate(array $tokens): void
    {
        $failure = null;
        foreach ($tokens as $key => $token) {
            try {
                $this->queryCache->activateCollection($key, $token);
            } catch (Throwable $error) {
                $failure ??= $error;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    public function isMutation(Event $event): bool
    {
        return \in_array($event, [
            Event::CollectionCreate,
            Event::CollectionUpdate,
            Event::CollectionDelete,
            Event::AttributeCreate,
            Event::AttributesCreate,
            Event::AttributeUpdate,
            Event::AttributeDelete,
            Event::IndexCreate,
            Event::IndexRename,
            Event::IndexDelete,
            Event::DocumentPurge,
            Event::DocumentCreate,
            Event::DocumentsCreate,
            Event::DocumentUpdate,
            Event::DocumentsUpdate,
            Event::DocumentsUpsert,
            Event::DocumentDelete,
            Event::DocumentsDelete,
            Event::DocumentIncrease,
            Event::DocumentDecrease,
            Event::PermissionsCreate,
            Event::PermissionsDelete,
        ], true);
    }

    /**
     * Only an attribute event's relationship options name a related collection: a written
     * document's own `options` attribute is data.
     */
    private function isAttributeMutation(Event $event): bool
    {
        return \in_array($event, [
            Event::AttributeCreate,
            Event::AttributesCreate,
            Event::AttributeUpdate,
            Event::AttributeDelete,
        ], true);
    }

    /**
     * @return array<string, string>
     */
    private function scopedTokens(Event $event, mixed $data, Scope $scope): array
    {
        $tokens = [];
        foreach (\array_keys($this->extractCollections($event, $data)) as $collection) {
            $tokens[$this->queryCache->getCollectionKey($scope, (string) $collection)] = \bin2hex(\random_bytes(16));
        }

        return $tokens;
    }

    /**
     * @return array<string, true>
     */
    private function extractCollections(Event $event, mixed $data): array
    {
        $collections = [];

        if (\is_array($data)) {
            foreach ($data as $item) {
                foreach ($this->extractCollections($event, $item) as $collection => $present) {
                    $collections[$collection] = $present;
                }
            }

            return $collections;
        }

        if ($data instanceof Document) {
            if (\in_array($event, [
                Event::CollectionCreate,
                Event::CollectionUpdate,
                Event::CollectionDelete,
            ], true)) {
                $collection = $data->getId();
            } else {
                $collection = $data->getCollection();
                if ($collection === Database::METADATA) {
                    $collection = $data->getId();
                }
            }

            if ($collection !== '') {
                $collections[$collection] = true;
            }

            if (! $this->isAttributeMutation($event)) {
                return $collections;
            }

            $options = $data->getAttribute('options', []);
            if ($options instanceof Document) {
                $options = $options->getArrayCopy();
            }
            $related = \is_array($options) ? ($options['relatedCollection'] ?? null) : null;
            if (\is_string($related) && $related !== '') {
                $collections[$related] = true;
            }

            return $collections;
        }

        if (\is_string($data) && $data !== '') {
            $collections[$data] = true;
        }

        return $collections;
    }
}
