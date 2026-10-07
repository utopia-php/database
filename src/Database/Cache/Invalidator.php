<?php

namespace Utopia\Database\Cache;

use Throwable;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Query\Hook;

/**
 * The query cache invalidation a database runs for its mutations. It is not a lifecycle hook: it is never silenced,
 * reads the raw write targets and runs before the hooks; addHook() makes it the database's invalidation.
 */
class Invalidator implements Hook
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
        $this->activate($tokens, $scope->writerTimeout);
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
            $scopes[$key] ??= $scope->withTenant($tenant);
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
     * @param  int  $writerTimeout  The writer timeout of the database the tokens were created through
     */
    public function activate(array $tokens, int $writerTimeout): void
    {
        $failure = null;
        foreach ($tokens as $key => $token) {
            try {
                $this->queryCache->activateCollection($key, $token, $writerTimeout);
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
            Event::AttributeRename,
            Event::AttributeDelete,
            Event::IndexCreate,
            Event::IndexesCreate,
            Event::IndexRename,
            Event::IndexDelete,
            Event::DocumentPurge,
            Event::DocumentCreate,
            Event::DocumentsCreate,
            Event::DocumentUpdate,
            Event::DocumentsUpdate,
            Event::DocumentUpsert,
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
            Event::AttributeRename,
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
            $tokens[$this->queryCache->getCollectionKey($scope, (string) $collection)] = $this->queryCache->createToken();
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

            if (! $this->isAttributeMutation($event) || ! Attribute::isRelationship($data)) {
                return $collections;
            }

            $related = Attribute::fromDocument($data)->relationship?->relatedCollection;
            if ($related !== null) {
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
