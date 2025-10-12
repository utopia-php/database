<?php

namespace Utopia\Database;

/**
 * Single-pass document processor for read path
 * Combines decode + casting in one pass with proper filter support.
 */
class DocumentProcessor
{
    /**
     * @var array<string, array{decode: callable}>
     */
    private static array $filters = [];

    /**
     * Guard to ensure we only register filters once.
     */
    private static bool $initialized = false;

    public function __construct()
    {
        self::ensureInitialized();
    }

    private static function ensureInitialized(): void
    {
        if (self::$initialized) {
            return;
        }
        // Register standard filters (matching Database class filters semantics used on read)
        // Note: simdjson provides ~10-15% improvement on JSON parsing but throws exceptions
        // For production use with controlled JSON, consider json_decode for better compatibility
        self::$filters["json"] = [
            "decode" => function (mixed $value) {
                if (!is_string($value)) {
                    return $value;
                }

                // Use standard json_decode for reliability
                // (simdjson is faster but has compatibility issues with some edge cases)
                $value = json_decode($value, true) ?? [];

                if (is_array($value) && array_key_exists('$id', $value)) {
                    return new Document($value);
                }
                if (is_array($value)) {
                    // Manual loop faster than array_map for small arrays
                    foreach ($value as $i => $item) {
                        if (is_array($item) && array_key_exists('$id', $item)) {
                            $value[$i] = new Document($item);
                        }
                    }
                }
                return $value;
            },
        ];

        self::$filters["datetime"] = [
            "decode" => function (?string $value) {
                return DateTime::formatTz($value);
            },
        ];

        self::$initialized = true;
    }

    /**
     * Expose supported filter names for gating logic.
     *
     * @return array<string>
     */
    public static function getSupportedFilters(): array
    {
        self::ensureInitialized();
        return array_keys(self::$filters);
    }

    /**
     * Register adapter-aware decoders (spatial types) for single-pass processing.
     * Safe to call multiple times; overwrites existing entries.
     */
    public static function registerAdapterFilters(Adapter $adapter): void
    {
        self::ensureInitialized();

        self::$filters[Database::VAR_POINT] = [
            'decode' => function (?string $value) use ($adapter) {
                if ($value === null) {
                    return null;
                }
                return $adapter->decodePoint($value);
            },
        ];

        self::$filters[Database::VAR_LINESTRING] = [
            'decode' => function (?string $value) use ($adapter) {
                if ($value === null) {
                    return null;
                }
                return $adapter->decodeLinestring($value);
            },
        ];

        self::$filters[Database::VAR_POLYGON] = [
            'decode' => function (?string $value) use ($adapter) {
                if ($value === null) {
                    return null;
                }
                return $adapter->decodePolygon($value);
            },
        ];
    }

    /**
     * Process document for read (decode + casting) in a single pass.
     *
     * @param Document $collection
     * @param Document $document
     * @return Document
     */
    public function processRead(
        Document $collection,
        Document $document,
        ?callable $keyMapper = null,
        array $selections = [],
        bool $skipCasting = false
    ): Document {
        $attributes = $collection->getAttribute("attributes", []);

        // Pre-normalize relationship keys like Database::decode
        $relationships = \array_filter(
            $attributes,
            fn ($attribute) => ($attribute['type'] ?? '') === Database::VAR_RELATIONSHIP
        );
        if (!empty($relationships) && $keyMapper !== null) {
            foreach ($relationships as $relationship) {
                $rKey = $relationship['$id'] ?? '';
                if ($rKey === '') {
                    continue;
                }
                $mapped = $keyMapper($rKey);
                $hasOriginal = \array_key_exists($rKey, (array)$document);
                $hasMapped = is_string($mapped) && \array_key_exists($mapped, (array)$document);
                if ($hasOriginal || $hasMapped) {
                    $value = $document->getAttribute($rKey);
                    if ($value === null && $hasMapped) {
                        $value = $document->getAttribute($mapped);
                    }
                    if ($hasMapped) {
                        $document->removeAttribute($mapped);
                    }
                    $document->setAttribute($rKey, $value);
                }
            }
        }

        // Iterate attributes and skip relationships without creating a new array
        $filteredValues = [];
        foreach ($attributes as $attribute) {
            if (($attribute['type'] ?? '') === Database::VAR_RELATIONSHIP) {
                continue;
            }
            $key = $attribute['$id'] ?? "";
            $type = $attribute["type"] ?? "";
            $array = $attribute["array"] ?? false;
            $filters = $attribute["filters"] ?? [];

            if ($key === '$permissions') {
                continue;
            }

            // Prefer original key; fall back to adapter-mapped key if provided
            $value = $document->getAttribute($key);
            if ($value === null && $keyMapper !== null) {
                $mapped = $keyMapper($key);
                if (is_string($mapped) && $mapped !== $key) {
                    $value = $document->getAttribute($mapped);
                    if ($value !== null) {
                        $document->removeAttribute($mapped);
                    }
                }
            }

            if ($array) {
                // In a single pass, if DB returns arrays as JSON strings, normalize once
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    $value = \is_array($decoded) ? $decoded : $value;
                }
                if (!\is_array($value)) {
                    $value = $value === null ? [] : [$value];
                }

                $revFilters = empty($filters) ? [] : array_reverse($filters);
                foreach ($value as $i => $node) {
                    foreach ($revFilters as $filter) {
                        $node = $this->decodeAttribute($filter, $node);
                    }
                    $value[$i] = $skipCasting ? $node : $this->castNode($type, $node);
                }
                $filteredValues[$key] = $value;
                if (empty($selections) || \in_array($key, $selections, true) || \in_array('*', $selections, true)) {
                    $document->setAttribute($key, $value);
                }
            } else {
                // Apply filters for non-array values
                if (!empty($filters)) {
                    foreach (array_reverse($filters) as $filter) {
                        $value = $this->decodeAttribute($filter, $value);
                    }
                }
                $final = $skipCasting ? $value : $this->castNode($type, $value);
                $filteredValues[$key] = $final;
                if (empty($selections) || \in_array($key, $selections, true) || \in_array('*', $selections, true)) {
                    $document->setAttribute($key, $final);
                }
            }
        }

        // Apply internal attributes at the end to keep behavior consistent
        foreach (Database::INTERNAL_ATTRIBUTES as $attribute) {
            $key = $attribute['$id'] ?? "";
            $type = $attribute["type"] ?? "";
            $array = $attribute["array"] ?? false;
            $filters = $attribute["filters"] ?? [];

            if ($key === '$permissions') {
                continue;
            }

            $value = $document->getAttribute($key);
            if ($value === null && $keyMapper !== null) {
                $mapped = $keyMapper($key);
                if (is_string($mapped) && $mapped !== $key) {
                    $value = $document->getAttribute($mapped);
                    if ($value !== null) {
                        $document->removeAttribute($mapped);
                    }
                }
            }

            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    $value = \is_array($decoded) ? $decoded : $value;
                }
                if (!\is_array($value)) {
                    $value = $value === null ? [] : [$value];
                }

                $revFilters = empty($filters) ? [] : array_reverse($filters);
                foreach ($value as $i => $node) {
                    foreach ($revFilters as $filter) {
                        $node = $this->decodeAttribute($filter, $node);
                    }
                    $value[$i] = $skipCasting ? $node : $this->castNode($type, $node);
                }
                if (empty($selections) || \in_array($key, $selections, true) || \in_array('*', $selections, true)) {
                    $document->setAttribute($key, $value);
                }
            } else {
                if (!empty($filters)) {
                    foreach (array_reverse($filters) as $filter) {
                        $value = $this->decodeAttribute($filter, $value);
                    }
                }
                $final = $skipCasting ? $value : $this->castNode($type, $value);
                if (empty($selections) || \in_array($key, $selections, true) || \in_array('*', $selections, true)) {
                    $document->setAttribute($key, $final);
                }
            }
        }

        // Relationship selection semantics: if selecting relationship attributes, also include
        // non-relationship attributes even if not explicitly selected.
        $hasRelationshipSelections = false;
        if (!empty($selections)) {
            foreach ($selections as $sel) {
                if (\str_contains($sel, '.')) {
                    $hasRelationshipSelections = true;
                    break;
                }
            }
        }
        if ($hasRelationshipSelections && !empty($selections) && !\in_array('*', $selections, true)) {
            foreach ($collection->getAttribute('attributes', []) as $attribute) {
                $key = $attribute['$id'] ?? '';
                if (($attribute['type'] ?? '') === Database::VAR_RELATIONSHIP || $key === '$permissions') {
                    continue;
                }
                if (!\in_array($key, $selections, true) && \array_key_exists($key, $filteredValues)) {
                    $document->setAttribute($key, $filteredValues[$key]);
                }
            }
        }

        return $document;
    }

    /**
     * Prepare a per-collection plan for batch processing.
     *
     * @return array{
     *   relationships: array<int, array{key:string, mapped:?string}>,
     *   attrs: array<int, array{key:string, mapped:?string, type:string, array:bool, filters:array<int,string>, selected:bool}>,
     *   internals: array<int, array{key:string, type:string, array:bool, filters:array<int,string>, selected:bool}>,
     *   skipCasting: bool
     * }
     */
    private function preparePlan(Document $collection, ?callable $keyMapper, array $selections, bool $skipCasting): array
    {
        $attributes = $collection->getAttribute('attributes', []);

        $relationships = [];
        $attrs = [];
        foreach ($attributes as $attr) {
            $type = $attr['type'] ?? '';
            $key = $attr['$id'] ?? '';
            if ($type === Database::VAR_RELATIONSHIP) {
                $mapped = ($keyMapper !== null) ? $keyMapper($key) : null;
                $relationships[] = [
                    'key' => $key,
                    'mapped' => (is_string($mapped) && $mapped !== $key) ? $mapped : null,
                ];
                continue;
            }
            $mapped = ($keyMapper !== null) ? $keyMapper($key) : null;
            $attrs[] = [
                'key' => $key,
                'mapped' => (is_string($mapped) && $mapped !== $key) ? $mapped : null,
                'type' => $type,
                'array' => (bool)($attr['array'] ?? false),
                'filters' => array_reverse($attr['filters'] ?? []),
                'selected' => empty($selections) || in_array($key, $selections, true) || in_array('*', $selections, true),
            ];
        }

        $internals = [];
        foreach (Database::INTERNAL_ATTRIBUTES as $attr) {
            $key = $attr['$id'] ?? '';
            if ($key === '$permissions') {
                continue;
            }
            $internals[] = [
                'key' => $key,
                'type' => $attr['type'] ?? '',
                'array' => (bool)($attr['array'] ?? false),
                'filters' => array_reverse($attr['filters'] ?? []),
                'selected' => empty($selections) || in_array($key, $selections, true) || in_array('*', $selections, true),
            ];
        }

        // Detect relationship selections
        $hasRelationshipSelections = false;
        if (!empty($selections)) {
            foreach ($selections as $sel) {
                if (\str_contains($sel, '.')) {
                    $hasRelationshipSelections = true;
                    break;
                }
            }
        }

        return [
            'relationships' => $relationships,
            'attrs' => $attrs,
            'internals' => $internals,
            'skipCasting' => $skipCasting,
            'hasRelSelects' => $hasRelationshipSelections && !empty($selections) && !\in_array('*', $selections, true),
        ];
    }

    /**
     * Batch version of processRead preserving parity semantics.
     *
     * @param array<int, Document> $documents
     * @return array<int, Document>
     */
    public function processReadBatch(
        Document $collection,
        array $documents,
        ?callable $keyMapper = null,
        array $selections = [],
        bool $skipCasting = false
    ): array {
        if (empty($documents)) {
            return $documents;
        }

        $plan = $this->preparePlan($collection, $keyMapper, $selections, $skipCasting);

        foreach ($documents as $idx => $document) {
            if (!$document instanceof Document) {
                continue;
            }

            // Relationship key normalization
            if (!empty($plan['relationships'])) {
                foreach ($plan['relationships'] as $rel) {
                    $key = $rel['key'];
                    $mapped = $rel['mapped'] ?? null;
                    $hasOriginal = array_key_exists($key, (array)$document);
                    $hasMapped = $mapped && array_key_exists($mapped, (array)$document);
                    if ($hasOriginal || $hasMapped) {
                        $value = $document->getAttribute($key);
                        if ($value === null && $hasMapped) {
                            $value = $document->getAttribute($mapped);
                        }
                        if ($hasMapped) {
                            $document->removeAttribute($mapped);
                        }
                        $document->setAttribute($key, $value);
                    }
                }
            }

            // Regular attributes
            $filteredValues = [];
            foreach ($plan['attrs'] as $a) {
                $key = $a['key'];
                if ($key === '$permissions') {
                    continue;
                }
                $value = $document->getAttribute($key);
                if ($value === null && !empty($a['mapped'])) {
                    $value = $document->getAttribute($a['mapped']);
                    if ($value !== null) {
                        $document->removeAttribute($a['mapped']);
                    }
                }

                if ($a['array']) {
                    if (is_string($value)) {
                        $decoded = json_decode($value, true);
                        $value = is_array($decoded) ? $decoded : $value;
                    }
                    if (!is_array($value)) {
                        $value = $value === null ? [] : [$value];
                    }
                    foreach ($value as $i => $node) {
                        foreach ($a['filters'] as $filter) {
                            $node = $this->decodeAttribute($filter, $node);
                        }
                        $value[$i] = $plan['skipCasting'] ? $node : $this->castNode($a['type'], $node);
                    }
                    $filteredValues[$key] = $value;
                    if ($a['selected']) {
                        $document->setAttribute($key, $value);
                    }
                } else {
                    foreach ($a['filters'] as $filter) {
                        $value = $this->decodeAttribute($filter, $value);
                    }
                    $final = $plan['skipCasting'] ? $value : $this->castNode($a['type'], $value);
                    $filteredValues[$key] = $final;
                    if ($a['selected']) {
                        $document->setAttribute($key, $final);
                    }
                }
            }

            // Internal attributes
            foreach ($plan['internals'] as $a) {
                $key = $a['key'];
                $value = $document->getAttribute($key);

                if ($a['array']) {
                    if (is_string($value)) {
                        $decoded = json_decode($value, true);
                        $value = is_array($decoded) ? $decoded : $value;
                    }
                    if (!is_array($value)) {
                        $value = $value === null ? [] : [$value];
                    }
                    foreach ($value as $i => $node) {
                        foreach ($a['filters'] as $filter) {
                            $node = $this->decodeAttribute($filter, $node);
                        }
                        $value[$i] = $plan['skipCasting'] ? $node : $this->castNode($a['type'], $node);
                    }
                    if ($a['selected']) {
                        $document->setAttribute($key, $value);
                    }
                } else {
                    foreach ($a['filters'] as $filter) {
                        $value = $this->decodeAttribute($filter, $value);
                    }
                    $final = $plan['skipCasting'] ? $value : $this->castNode($a['type'], $value);
                    if ($a['selected']) {
                        $document->setAttribute($key, $final);
                    }
                }
            }

            // Relationship selection semantic adjustment
            if (!empty($plan['hasRelSelects'])) {
                foreach ($plan['attrs'] as $a) {
                    if ($a['selected']) {
                        continue;
                    }
                    $key = $a['key'];
                    if (\array_key_exists($key, $filteredValues)) {
                        $document->setAttribute($key, $filteredValues[$key]);
                    }
                }
            }

            $documents[$idx] = $document;
        }

        return $documents;
    }

    /**
     * Apply a decode filter to a value
     *
     * @param string $filter
     * @param mixed $value
     * @return mixed
     */
    protected function decodeAttribute(string $filter, mixed $value): mixed
    {
        if (!array_key_exists($filter, self::$filters)) {
            return $value; // Unknown filter, pass through
        }
        return self::$filters[$filter]["decode"]($value);
    }

    private function castNode(string $type, mixed $node): mixed
    {
        // Preserve null values like legacy decode does
        if ($node === null) {
            return null;
        }

        switch ($type) {
            case Database::VAR_ID:
                return (string) $node;
            case Database::VAR_BOOLEAN:
                return (bool) $node;
            case Database::VAR_INTEGER:
                return (int) $node;
            case Database::VAR_FLOAT:
                return (float) $node;
            default:
                return $node;
        }
    }

    /**
     * Add a custom filter
     *
     * @param string $name
     * @param callable $decode
     * @return void
     */
    public static function addFilter(string $name, callable $decode): void
    {
        self::$filters[$name] = ["decode" => $decode];
    }
}
