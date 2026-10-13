<?php

namespace Utopia\Database\Trait;

use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\Authorization\Input;
use Utopia\Database\Validator\Query\Join as JoinValidator;
use Utopia\Query\Method;
use Utopia\Query\Query as BaseQuery;
use Utopia\Query\Schema\IndexType;

trait Joins
{
    /**
     * A one-to-many parent attribute, or the child side of a two-way many-to-one, has no column: the related
     * collection stores this document's ID under the two-way key. A join comparing such an attribute with the related
     * `$id` compares this document's `$id` with that key instead. Rewritten joins are copies of the caller's.
     *
     * @param  array<Query>  $queries
     * @return array<Query>
     */
    private function rewriteRelationshipJoins(Collection $collection, array $queries): array
    {
        $relationships = null;

        foreach ($queries as $index => $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $relationships ??= self::joinableRelationships($collection);
            if ($relationships === []) {
                return $queries;
            }

            $joined = $query->getAttribute();
            $alias = $query->getAlias();
            $rewritten = null;

            foreach ($query->getJoinOnQueries() as $position => $condition) {
                $column = self::relationshipJoinColumn($condition, $joined, $alias, $relationships);
                if ($column === null) {
                    continue;
                }

                $rewritten ??= clone $query;
                $rewritten->getJoinOnQueries()[$position]->setValues([Document::ID, $condition->getValues()[1] ?? '=', $alias.'.'.$column]);
            }

            if ($rewritten !== null) {
                $queries[$index] = $rewritten;
            }
        }

        return $queries;
    }

    /**
     * @return array<string, Attribute>
     */
    private static function joinableRelationships(Collection $collection): array
    {
        $relationships = [];
        foreach (self::relationshipAttributes($collection) as $attribute) {
            $relationship = $attribute->relationship;
            if ($relationship === null || $relationship->twoWayKey === null || $relationship->twoWayKey === '') {
                continue;
            }

            $storedByRelated = match ($relationship->type) {
                RelationshipType::OneToMany => $attribute->side === RelationshipSide::Parent,
                RelationshipType::ManyToOne => $attribute->side === RelationshipSide::Child && $relationship->twoWay,
                RelationshipType::OneToOne, RelationshipType::ManyToMany => false,
            };

            if ($storedByRelated) {
                $relationships[$attribute->key] = $attribute;
            }
        }

        return $relationships;
    }

    /**
     * @param  array<string, Attribute>  $relationships
     */
    private static function relationshipJoinColumn(BaseQuery $condition, string $joined, string $alias, array $relationships): ?string
    {
        if ($condition->getMethod() !== Method::On) {
            return null;
        }

        $values = $condition->getValues();
        $left = $values[0] ?? null;
        $right = $values[2] ?? null;
        if (! \is_string($left) || ($right !== Document::ID && $right !== $alias.'.'.Document::ID)) {
            return null;
        }

        $relationship = ($relationships[$left] ?? null)?->relationship;
        if ($relationship === null || $relationship->relatedCollection !== $joined) {
            return null;
        }

        return $relationship->twoWayKey;
    }

    /**
     * The collections the join queries name, each loaded once, by the id the join names it with. A
     * read resolves them once and hands them to validation, authorization, the adapter and decoding.
     *
     * @param  array<Query>  $queries
     * @return array<string, Collection>
     *
     * @throws QueryException
     */
    private function resolveJoinedCollections(array $queries): array
    {
        $collections = [];

        foreach ($queries as $query) {
            if (! $query->getMethod()->isJoin()) {
                continue;
            }

            $id = $query->getAttribute();
            if ($id === '' || isset($collections[$id])) {
                continue;
            }

            $collection = $this->silent(fn () => $this->findCollection($id));
            if ($collection === null) {
                throw new QueryException("Joined collection '{$id}' not found");
            }

            $collections[$id] = $collection;
        }

        return $collections;
    }

    /**
     * @param  array<Query>  $queries
     * @param  list<Attribute>  $relationships
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array{0: Document, 1: array<Query>, 2: bool}|null
     */
    private function prepareFilterJoinQueries(
        Document $collection,
        array $queries,
        array $relationships,
        bool $collectionGranted,
        ?array $joinedCollections = null,
    ): ?array {
        $parsed = Query::groupByType($queries);
        $filters = $parsed->filters;
        $joins = $parsed->joins;

        if (! empty($joins)) {
            if (! $this->adapter->supports(Capability::Joins)) {
                throw new QueryException('Join queries are not supported by this adapter');
            }

            $this->assertJoinCount($joins);

            $joinedCollections ??= $this->resolveJoinedCollections($joins);
            $collection = $this->withJoinIndexed($this->withJoinAuthorization(
                $collection,
                $this->authorizeJoins($joins, PermissionType::Read, $joinedCollections),
                $collectionGranted,
            ), $joins, $joinedCollections);
        }

        $queries = \array_merge($filters, $joins);
        if ($queries !== []) {
            $queries = $this->convertQueries($collection, $queries, $this->joinedCollectionsByAlias($joins, $joinedCollections));
        }

        $convertedQueries = $this->relationshipHook !== null
            ? $this->relationshipHook->convertQueries($relationships, $queries, $collection)
            : $queries;

        if ($convertedQueries === null) {
            return null;
        }

        return [$collection, $convertedQueries, $collectionGranted && empty($joins)];
    }

    /**
     * The join cap the Join validator enforces holds without validation too: every join is one
     * more table the engine plans and the permission filters check.
     *
     * @param  array<Query>  $joins
     *
     * @throws QueryException
     */
    private function assertJoinCount(array $joins): void
    {
        $validator = new JoinValidator();
        if (! $validator->isValidCount(\count($joins))) {
            throw new QueryException($validator->getDescription());
        }
    }

    /**
     * Set on the collection handed to the adapter for a join read when the caller
     * holds the collection-level permission, so its rows are not filtered per document.
     */
    public const string COLLECTION_GRANTED = 'collectionGranted';

    /**
     * Maps each joined table to whether the adapter filters its rows per document.
     */
    public const string JOIN_DOCUMENT_SECURITY = 'joinDocumentSecurity';

    /**
     * Each joined collection is authorized once, however many joins read it.
     *
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array<string, bool>
     */
    private function authorizeJoins(array $joins, PermissionType $forPermission, ?array $joinedCollections = null): array
    {
        $joinedCollections ??= $this->resolveJoinedCollections($joins);
        $joinDocumentSecurity = [];
        $authorized = [];

        foreach ($joins as $joinQuery) {
            $joinCollectionId = $joinQuery->getAttribute();
            if (isset($authorized[$joinCollectionId])) {
                continue;
            }
            $authorized[$joinCollectionId] = true;

            $joinCollection = $joinedCollections[$joinCollectionId] ?? null;

            if ($joinCollection === null) {
                throw new QueryException("Joined collection '{$joinCollectionId}' not found");
            }

            $granted = $this->authorization->isValid(new Input($forPermission, $joinCollection->getPermissionsByType($forPermission)));
            $documentSecurity = (bool) $joinCollection->getAttribute('documentSecurity', false);

            if (! $granted && ! $documentSecurity) {
                throw new AuthorizationException("Unauthorized access to joined collection '{$joinCollectionId}'");
            }

            foreach ($this->joinDocumentSecurityKeys($joinCollectionId, $joinCollection) as $key) {
                $joinDocumentSecurity[$key] = ! $granted;
            }
        }

        return $joinDocumentSecurity;
    }

    /**
     * @return list<string>
     */
    private function joinDocumentSecurityKeys(string $joinCollectionId, Document $joinCollection): array
    {
        $keys = [
            $joinCollectionId,
            $joinCollection->getId(),
            $this->adapter->filter($joinCollectionId),
            $this->adapter->filter($joinCollection->getId()),
        ];

        return \array_values(\array_unique(\array_filter(
            $keys,
            static fn (string $key): bool => $key !== '',
        )));
    }

    /**
     * @param  array<string, bool>  $joinDocumentSecurity
     */
    private function withJoinAuthorization(Document $collection, array $joinDocumentSecurity, bool $collectionGranted): Document
    {
        if ($joinDocumentSecurity === []) {
            return $collection;
        }

        $adapterCollection = clone $collection;
        $adapterCollection->setAttribute(self::COLLECTION_GRANTED, $collectionGranted);
        $adapterCollection->setAttribute(self::JOIN_DOCUMENT_SECURITY, $joinDocumentSecurity);

        return $adapterCollection;
    }

    /**
     * Maps each joined collection, as its join query names it, to the attributes a join without a
     * select returns under the join's alias.
     */
    public const string JOIN_ATTRIBUTES = 'joinAttributes';

    /**
     * Relationship attributes are left out: only some sides of a relationship have a column, and a
     * join does not populate related documents.
     *
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     */
    private function withJoinAttributes(Document $collection, array $joins, ?array $joinedCollections = null): Document
    {
        if ($joins === []) {
            return $collection;
        }

        $joinedCollections ??= $this->resolveJoinedCollections($joins);

        $joinAttributes = [];
        foreach ($joins as $join) {
            $joinCollectionId = $join->getAttribute();
            if (isset($joinAttributes[$joinCollectionId])) {
                continue;
            }

            $keys = [];
            if (isset($joinedCollections[$joinCollectionId])) {
                foreach (Collection::fromDocument($joinedCollections[$joinCollectionId])->attributes() as $attribute) {
                    if ($attribute->relationship === null) {
                        $keys[] = $attribute->key;
                    }
                }
            }
            $joinAttributes[$joinCollectionId] = $keys;
        }

        $adapterCollection = clone $collection;
        $adapterCollection->setAttribute(self::JOIN_ATTRIBUTES, $joinAttributes);

        return $adapterCollection;
    }

    /**
     * Under shared tables, maps each joined collection, as its join query names it, to the attributes one of its key
     * or unique indexes leads with: the ones a join can look its rows up by.
     */
    public const string JOIN_INDEXED = 'joinIndexed';

    private const array LOOKUP_INDEX_TYPES = [IndexType::Key, IndexType::Index, IndexType::Unique];

    /**
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     */
    private function withJoinIndexed(Document $collection, array $joins, ?array $joinedCollections = null): Document
    {
        if ($joins === [] || ! $this->adapter->hasSharedTables()) {
            return $collection;
        }

        $joinedCollections ??= $this->resolveJoinedCollections($joins);

        $joinIndexed = [];
        foreach ($joins as $join) {
            $joinCollectionId = $join->getAttribute();
            if (isset($joinIndexed[$joinCollectionId]) || ! isset($joinedCollections[$joinCollectionId])) {
                continue;
            }

            $leads = [];
            foreach (Collection::fromDocument($joinedCollections[$joinCollectionId])->indexes() as $index) {
                if (\in_array($index->type, self::LOOKUP_INDEX_TYPES, true) && isset($index->attributes[0])) {
                    $leads[] = $index->attributes[0];
                }
            }
            $joinIndexed[$joinCollectionId] = \array_values(\array_unique($leads));
        }

        $adapterCollection = clone $collection;
        $adapterCollection->setAttribute(self::JOIN_INDEXED, $joinIndexed);

        return $adapterCollection;
    }

    /**
     * The collection each join reads, by the alias its values come back under.
     *
     * @param  array<Query>  $joins
     * @param  array<string, Document>|null  $joinedCollections  The collection each join names, by its id
     * @return array<string, Document>
     */
    private function joinedCollectionsByAlias(array $joins, ?array $joinedCollections = null): array
    {
        if ($joins === []) {
            return [];
        }

        $joinedCollections ??= $this->resolveJoinedCollections($joins);

        $collections = [];
        foreach ($joins as $join) {
            $collections[$join->getAlias()] = $joinedCollections[$join->getAttribute()] ?? new Document();
        }

        return $collections;
    }

    /**
     * The `alias.$id` of each join whose attributes a select names without it or `alias.*`, when an
     * outer join can leave a joined row unmatched: the joined `$id` is what tells an unmatched row
     * from a matched one when the row is decoded, so it is selected for that and left out of the
     * result.
     *
     * @param  array<Query>  $selects
     * @param  array<Query>  $joins
     * @param  array<string, Document>  $joinedCollections  The collection each join alias reads
     * @return list<string>
     */
    private function outerJoinIdSelections(array $selects, array $joins, array $joinedCollections): array
    {
        if ($selects === [] || $joinedCollections === []) {
            return [];
        }

        $outer = false;
        foreach ($joins as $join) {
            if (\in_array($join->getMethod(), [Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin], true)) {
                $outer = true;
                break;
            }
        }
        if (! $outer) {
            return [];
        }

        $selectedAliases = [];
        foreach ($selects as $select) {
            foreach ($select->getValues() as $value) {
                if (! \is_string($value)) {
                    continue;
                }
                if ($value === '*') {
                    return [];
                }
                $dot = \strpos($value, '.');
                if ($dot !== false) {
                    $selectedAliases[\substr($value, 0, $dot)][\substr($value, $dot + 1)] = true;
                }
            }
        }

        $ids = [];
        foreach ($selectedAliases as $alias => $attributes) {
            if (isset($joinedCollections[$alias]) && ! isset($attributes[Document::ID]) && ! isset($attributes['*'])) {
                $ids[] = $alias.'.'.Document::ID;
            }
        }

        return $ids;
    }
}
