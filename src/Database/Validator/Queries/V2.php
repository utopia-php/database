<?php

namespace Utopia\Database\Validator\Queries;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Alias as AliasValidator;
use Utopia\Database\Validator\AsQuery as AsValidator;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Sequence;
use Utopia\Validator;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

class V2 extends Validator
{
    protected string $message = 'Invalid query';

    /**
     * @var array<mixed>
     */
    protected array $schema = [];

    protected int $maxQueriesCount;

    private int $maxValuesCount;

    protected int $maxLimit;

    protected int $maxOffset;

    protected QueryContext $context;

    protected \DateTime $minAllowedDate;

    protected \DateTime $maxAllowedDate;
    protected string $idAttributeType;
    protected int $vectors = 0;

    /**
     * @var array<string>
     */
    protected array $joinsAliasOrder = [Query::DEFAULT_ALIAS];

    /**
     * @param QueryContext $context
     * @param string $idAttributeType
     * @param int $maxValuesCount
     * @param int $maxQueriesCount
     * @param \DateTime $minAllowedDate
     * @param \DateTime $maxAllowedDate
     * @param int $maxLimit
     * @param int $maxOffset
     * @param bool $supportForAttributes
     * @param int $maxUIDLength
     * @param array<string,bool> $joinsCollectionsIds
     * @throws Exception
     */
    public function __construct(
        QueryContext $context,
        string $idAttributeType,
        int $maxValuesCount = 100,
        int $maxQueriesCount = 0,
        \DateTime $minAllowedDate = new \DateTime('0000-01-01'),
        \DateTime $maxAllowedDate = new \DateTime('9999-12-31'),
        int $maxLimit = PHP_INT_MAX,
        int $maxOffset = PHP_INT_MAX,
        protected bool $supportForAttributes = true,
        protected int $maxUIDLength = Database::MAX_UID_DEFAULT_LENGTH,
        protected array $joinsCollectionsIds = []
    ) {
        $this->context = $context;
        $this->idAttributeType = $idAttributeType;
        $this->maxQueriesCount = $maxQueriesCount;
        $this->maxValuesCount = $maxValuesCount;
        $this->maxLimit = $maxLimit;
        $this->maxOffset = $maxOffset;
        $this->minAllowedDate = $minAllowedDate;
        $this->maxAllowedDate = $maxAllowedDate;

        /**
         * Since $context includes Documents , clone if original data is changes.
         */
        foreach ($context->getCollections() as $collection) {
            $collection = clone $collection;

            $attributes = $collection->getAttribute('attributes', []);

            $attributes[] = new Document([
                '$id' => '$id',
                'key' => '$id',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]);

            $attributes[] = new Document([
                '$id' => '$sequence',
                'key' => '$sequence',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]);

            $attributes[] = new Document([
                '$id' => '$createdAt',
                'key' => '$createdAt',
                'type' => Database::VAR_DATETIME,
                'array' => false,
            ]);

            $attributes[] = new Document([
                '$id' => '$updatedAt',
                'key' => '$updatedAt',
                'type' => Database::VAR_DATETIME,
                'array' => false,
            ]);

            foreach ($attributes as $attribute) {
                $key = $attribute->getAttribute('key', $attribute->getAttribute('$id'));
                $this->schema[$collection->getId()][$key] = $attribute->getArrayCopy();
            }
        }
    }

    /**
     * Get Description.
     *
     * Returns validator description
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     */
    public function isArray(): bool
    {
        return true;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    /**
     * @param  array<Query|string>  $value
     *
     * @throws \Utopia\Database\Exception\Query|\Throwable
     */
    public function isValid($value, string $scope = ''): bool
    {
        try {
            if (! is_array($value)) {
                throw new \Exception('Queries must be an array');
            }

            if (! array_is_list($value)) {
                throw new \Exception('Queries must be an array list');
            }

            if ($this->maxQueriesCount > 0 && \count($value) > $this->maxQueriesCount) {
                throw new \Exception('Queries count is greater than '.$this->maxQueriesCount);
            }

            foreach ($value as $query) {
                if (!$query instanceof Query) {
                    try {
                        $query = Query::parse($query);
                    } catch (\Throwable $e) {
                        throw new \Exception('Invalid query: ' . $e->getMessage());
                    }
                }

                $this->validateAlias($query);

                if ($query->isNested()) {
                    if (! $this->isValid($query->getValues(), $scope)) {
                        throw new \Exception($this->message);
                    }
                }

                if ($scope === 'joins') {
                    if (!in_array($query->getAlias(), $this->joinsAliasOrder) || !in_array($query->getRightAlias(), $this->joinsAliasOrder)) {
                        throw new \Exception('Invalid query: '.\ucfirst($query->getMethod()).' alias reference in join has not been defined.');
                    }
                }

                $method = $query->getMethod();

                switch ($method) {
                    case Query::TYPE_EQUAL:
                    case Query::TYPE_CONTAINS:
                    case Query::TYPE_NOT_CONTAINS:
                    case Query::TYPE_EXISTS:
                    case Query::TYPE_NOT_EXISTS:
                        if ($this->isEmpty($query->getValues())) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require at least one value.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);

                        break;

                    case Query::TYPE_DISTANCE_EQUAL:
                    case Query::TYPE_DISTANCE_NOT_EQUAL:
                    case Query::TYPE_DISTANCE_GREATER_THAN:
                    case Query::TYPE_DISTANCE_LESS_THAN:
                        if (count($query->getValues()) !== 1 || !is_array($query->getValues()[0]) || count($query->getValues()[0]) !== 3) {
                            throw new \Exception('Distance query requires [[geometry, distance]] parameters');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);
                        break;

                    case Query::TYPE_CROSSES:
                    case Query::TYPE_NOT_CROSSES:
                    case Query::TYPE_INTERSECTS:
                    case Query::TYPE_NOT_INTERSECTS:
                    case Query::TYPE_OVERLAPS:
                    case Query::TYPE_NOT_OVERLAPS:
                    case Query::TYPE_TOUCHES:
                    case Query::TYPE_NOT_TOUCHES :
                        if ($this->isEmpty($query->getValues())) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require at least one value.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);
                        break;

                    case Query::TYPE_NOT_EQUAL:
                    case Query::TYPE_LESSER:
                    case Query::TYPE_LESSER_EQUAL:
                    case Query::TYPE_GREATER:
                    case Query::TYPE_GREATER_EQUAL:
                    case Query::TYPE_SEARCH:
                    case Query::TYPE_NOT_SEARCH:
                    case Query::TYPE_STARTS_WITH:
                    case Query::TYPE_NOT_STARTS_WITH:
                    case Query::TYPE_ENDS_WITH:
                    case Query::TYPE_NOT_ENDS_WITH:
                    case Query::TYPE_REGEX:
                        if (count($query->getValues()) != 1) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require exactly one value.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);
                        $this->validateFulltextIndex($query);

                        break;

                    case Query::TYPE_BETWEEN:
                    case Query::TYPE_NOT_BETWEEN:
                        if (count($query->getValues()) != 2) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require exactly two values.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);

                        break;

                    case Query::TYPE_IS_NULL:
                    case Query::TYPE_IS_NOT_NULL:
                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);

                        break;

                    case Query::TYPE_OR:
                    case Query::TYPE_AND:
                        $this->validateFilterQueries($query);
                        $filters = Query::getFilterQueries($query->getValues());

                        if (count($filters) < 2) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require at least two queries');
                        }

                        break;

                    case Query::TYPE_ELEM_MATCH:
                        if ($this->supportForAttributes) {
                            throw new \Exception(\ucfirst($method).' is not supported by the database');
                        }

                        $this->validateFilterQueries($query);

                        $filters = Query::getFilterQueries($query->getValues());

                        if (count($filters) < 1) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require at least one queries');
                        }

                        break;

                    case Query::TYPE_INNER_JOIN:
                    case Query::TYPE_LEFT_JOIN:
                    case Query::TYPE_RIGHT_JOIN:
                        if (($this->joinsCollectionsIds[$query->getCollectionId()] ?? false) !== true) {
                            throw new \Exception('Invalid query: Cannot ' . ucfirst($method) . ' this table.');
                        }

                        $this->joinsAliasOrder[] = $query->getAlias();

                        $this->validateFilterQueries($query);

                        if (! $this->isValid($query->getValues(), 'joins')) {
                            throw new \Exception($this->message);
                        }

                        if (! $this->isRelationExist($query->getValues(), $query->getAlias())) {
                            throw new \Exception('Invalid query: At least one relation query is required on the joined collection.');
                        }

                        break;

                    case Query::TYPE_RELATION_EQUAL:
                        if ($scope !== 'joins') {
                            throw new \Exception('Invalid query: Relations are only valid within joins.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateAttributeExist($query->getAttributeRight(), $query->getRightAlias());

                        break;

                    case Query::TYPE_LIMIT:
                        $validator = new Limit($this->maxLimit);
                        if (! $validator->isValid($query)) {
                            throw new \Exception($validator->getDescription());
                        }

                        break;

                    case Query::TYPE_OFFSET:
                        $validator = new Offset($this->maxOffset);
                        if (! $validator->isValid($query)) {
                            throw new \Exception($validator->getDescription());
                        }

                        break;

                    case Query::TYPE_SELECT:
                        if (empty($query->getAttribute())) {
                            throw new \Exception('Invalid query: '.\ucfirst(Query::TYPE_SELECT).' queries requires an attribute');
                        }

                        $asValidator = new AsValidator($query->getAttribute());
                        if (! $asValidator->isValid($query->getAs())) {
                            throw new \Exception('Invalid query: '.\ucfirst(Query::TYPE_SELECT).' '.$asValidator->getDescription());
                        }

                        if ($query->getAttribute() !== '*') {
                            $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        }

                        break;

                    case Query::TYPE_ORDER_RANDOM:

                        break;

                    case Query::TYPE_ORDER_ASC:
                    case Query::TYPE_ORDER_DESC:
                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias(), $query->getMethod());

                        break;

                    case Query::TYPE_CURSOR_AFTER:
                    case Query::TYPE_CURSOR_BEFORE:
                        $validator = new Cursor($this->maxUIDLength);
                        if (! $validator->isValid($query)) {
                            throw new \Exception($validator->getDescription());
                        }

                        break;

                    case Query::TYPE_VECTOR_DOT:
                    case Query::TYPE_VECTOR_COSINE:
                    case Query::TYPE_VECTOR_EUCLIDEAN:
                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());

                        if (count($query->getValues()) != 1) {
                            throw new \Exception(\ucfirst($method) . ' queries require exactly one vector value.');
                        }

                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);
                        break;

                    default:
                        throw new \Exception('Invalid query: Method not found ');
                }
            }

        } catch (\Throwable $e) {
            $this->message = $e->getMessage();
            return false;
        }

        return true;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function isEmpty(array $values): bool
    {
        if (count($values) === 0) {
            return true;
        }

        if (is_array($values[0]) && count($values[0]) === 0) {
            return true;
        }

        return false;
    }

    /**
     * @throws \Exception
     */
    protected function validateAttributeExist(string $attributeId, string $alias, string $method = ''): void
    {
        /**
         * This is for making query::select('$permissions')) pass
         */
        if ($attributeId === '$permissions' || $attributeId === '$collection') {
            return;
        }

        $collection = $this->context->getCollectionByAlias($alias);
        if ($collection->isEmpty()) {
            throw new \Exception('Invalid query: Unknown Alias context');
        }

        $isNested = false;

        if (\str_contains($attributeId, '.')) {
            /**
             * This attribute name has a special symbol `.` or is a relationship
             */
            if (empty($this->schema[$collection->getId()][$attributeId])) {
                /**
                 * relationships, just validate the top level.
                 * will validate each nested level during the recursive calls.
                 */
                $attributeId = \explode('.', $attributeId)[0];
                $isNested = true;
            }
        }

        $attribute = $this->schema[$collection->getId()][$attributeId] ?? [];

        if (empty($attribute) && !$this->supportForAttributes) {
            return; // Schemaless (Internal attributes have schema)
        }

        if (empty($attribute) && $this->supportForAttributes) {
            throw new \Exception('Invalid query: Attribute not found in schema: '.$attributeId);
        }

        if (\in_array('encrypt', $attribute['filters'] ?? [])) {
            throw new \Exception('Cannot query encrypted attribute: ' . $attributeId);
        }

        if ($isNested && !in_array($attribute['type'], [Database::VAR_RELATIONSHIP, Database::VAR_OBJECT])) {
            throw new \Exception('Only nested relationships allowed');
        }

        if ($isNested && \in_array($method, [Query::TYPE_ORDER_ASC, Query::TYPE_ORDER_DESC])) {
            throw new \Exception('Cannot order by nested attribute: ' . $attributeId);
        }
    }

    /**
     * @throws \Exception
     */
    protected function validateAlias(Query $query): void
    {
        if ($query->getAlias() !== '') {
            $validator = new AliasValidator();
            if (! $validator->isValid($query->getAlias())) {
                throw new \Exception('Query '.\ucfirst($query->getMethod()).': '.$validator->getDescription());
            }
        }

        if ($query->getRightAlias() !== '') {
            $validator = new AliasValidator();
            if (! $validator->isValid($query->getRightAlias())) {
                throw new \Exception('Query '.\ucfirst($query->getMethod()).': '.$validator->getDescription());
            }
        }
    }

    /**
     * @throws \Exception
     */
    protected function validateFilterQueries(Query $query): void
    {
        $filters = Query::getFilterQueries($query->getValues());

        if (count($query->getValues()) !== count($filters)) {
            throw new \Exception('Invalid query: '.\ucfirst($query->getMethod()).' queries can only contain filter queries');
        }
    }

    /**
     * @param string $attributeId
     * @param string $alias
     * @param array<mixed> $values
     * @param string $method
     * @return void
     * @throws \Exception
     */
    protected function validateValues(string $attributeId, string $alias, array $values, string $method): void
    {
        if (count($values) > $this->maxValuesCount) {
            throw new \Exception('Invalid query: Query on attribute has greater than '.$this->maxValuesCount.' values: '.$attributeId);
        }

        $collection = $this->context->getCollectionByAlias($alias);
        if ($collection->isEmpty()) {
            throw new \Exception('Unknown Alias context');
        }

        $isNested = false;

        if (\str_contains($attributeId, '.')) {
            /**
             * This attribute name has a special symbol `.` or is a relationship
             */
            if (empty($this->schema[$collection->getId()][$attributeId])) {
                /**
                 * relationships, just validate the top level.
                 * will validate each nested level during the recursive calls.
                 */
                $attributeId = \explode('.', $attributeId)[0];
                $isNested = true;
            }
        }

        $attribute = $this->schema[$collection->getId()][$attributeId] ?? [];
        if (empty($attribute) && !$this->supportForAttributes) {
            return;
        }

        /**
         * Skip value validation for nested relationship queries (e.g., author.age)
         * The values will be validated when querying the related collection
         */
        if ($attribute['type'] === Database::VAR_RELATIONSHIP && $isNested) {
            return;
        }

        $array = $attribute['array'] ?? false;
        $size = $attribute['size'] ?? 0;

        if (Query::isSpatialQuery($method) && !in_array($attribute['type'], Database::SPATIAL_TYPES, true)) {
            /**
             * If the query method is spatial-only, the attribute must be a spatial type
             */
            throw new \Exception('Invalid query: Spatial query "' . $method . '" cannot be applied on non-spatial attribute: ' . $attributeId);
        }

        if (Query::isVectorQuery($method) && $attribute['type'] !== Database::VAR_VECTOR) {
            throw new \Exception('Vector queries can only be used on vector attributes');
        }

        foreach ($values as $value) {
            $validator = null;

            switch ($attribute['type']) {
                case Database::VAR_ID:
                    $validator = new Sequence($this->idAttributeType, $attributeId === '$sequence');
                    break;

                case Database::VAR_STRING:
                case Database::VAR_VARCHAR:
                case Database::VAR_TEXT:
                case Database::VAR_MEDIUMTEXT:
                case Database::VAR_LONGTEXT:
                    $validator = new Text(0, 0);
                    break;

                case Database::VAR_INTEGER:
                    $signed = $attribute['signed'] ?? true;
                    $bits = $size >= 8 ? 64 : 32;
                    // For 64-bit unsigned, use signed since PHP doesn't support true 64-bit unsigned
                    $unsigned = !$signed && $bits < 64;
                    $validator = new Integer(false, $bits, $unsigned);
                    break;

                case Database::VAR_FLOAT:
                    $validator =  new FloatValidator();
                    break;

                case Database::VAR_BOOLEAN:
                    $validator = new Boolean();
                    break;

                case Database::VAR_DATETIME:
                    $validator = new DatetimeValidator(
                        min: $this->minAllowedDate,
                        max: $this->maxAllowedDate
                    );
                    break;

                case Database::VAR_RELATIONSHIP:
                    $validator = new Text(255, 0); // The query is always on uid
                    break;

                case Database::VAR_OBJECT:
                    // For dotted attributes on objects, validate as string (path queries)
                    if ($isNested) {
                        $validator = new Text(0, 0);
                        break;
                    }

                    if (
                        \in_array($method, [Query::TYPE_EQUAL, Query::TYPE_NOT_EQUAL, Query::TYPE_CONTAINS, Query::TYPE_NOT_CONTAINS], true) &&
                        !$this->isValidObjectQueryValues($value)
                    ) {
                        throw new \Exception('Invalid object query structure for attribute "'.$attributeId.'"');
                    }

                    continue 2;

                case Database::VAR_POINT:
                case Database::VAR_LINESTRING:
                case Database::VAR_POLYGON:
                    if (!is_array($value)) {
                        throw new \Exception('Spatial data must be an array');
                    }

                    continue 2;

                case Database::VAR_VECTOR:
                    if ($this->vectors > 0) {
                        throw new \Exception('Cannot use multiple vector queries in a single request');
                    }

                    $this->vectors++;

                    // For vector queries, validate that the value is an array of floats
                    if (!is_array($value)) {
                        throw new \Exception('Vector query value must be an array');
                    }

                    foreach ($value as $component) {
                        if (!is_numeric($component)) {
                            throw new \Exception('Vector query value must contain only numeric values');
                        }
                    }
                    // Check size match
                    if (count($value) !== $size) {
                        throw new \Exception("Vector query value must have {$size} elements");
                    }

                    continue 2;
                default:
                    throw new \Exception('Unknown Data type');
            }

            if (! $validator->isValid($value)) {
                throw new \Exception('Invalid query: Query value is invalid for attribute "'.$attributeId.'"');
            }
        }

        if ($attribute['type'] === 'relationship') {
            /**
             * We can not disable relationship query since we have logic that use it,
             * so instead we validate against the relation type
             */
            $options = $attribute['options'];

            if ($options['relationType'] === Database::RELATION_ONE_TO_ONE && $options['twoWay'] === false && $options['side'] === Database::RELATION_SIDE_CHILD) {
                throw new \Exception('Cannot query on virtual relationship attribute');
            }

            if ($options['relationType'] === Database::RELATION_ONE_TO_MANY && $options['side'] === Database::RELATION_SIDE_PARENT) {
                throw new \Exception('Cannot query on virtual relationship attribute');
            }

            if ($options['relationType'] === Database::RELATION_MANY_TO_ONE && $options['side'] === Database::RELATION_SIDE_CHILD) {
                throw new \Exception('Cannot query on virtual relationship attribute');
            }

            if ($options['relationType'] === Database::RELATION_MANY_TO_MANY) {
                throw new \Exception('Cannot query on virtual relationship attribute');
            }
        }

        if (
            ! $array &&
            in_array($method, [Query::TYPE_CONTAINS, Query::TYPE_NOT_CONTAINS]) &&
            $attribute['type'] !== Database::VAR_STRING &&
            $attribute['type'] !== Database::VAR_OBJECT &&
            !in_array($attribute['type'], Database::SPATIAL_TYPES)
        ) {
            throw new \Exception('Invalid query: Cannot query '.$method.' on attribute "'.$attributeId.'" because it is not an array, string, or object.');
        }

        if (
            $array &&
            !in_array($method, [
                Query::TYPE_CONTAINS,
                Query::TYPE_NOT_CONTAINS,
                Query::TYPE_IS_NULL,
                Query::TYPE_IS_NOT_NULL,
                Query::TYPE_EXISTS,
                Query::TYPE_NOT_EXISTS,
            ])
        ) {
            throw new \Exception('Invalid query: Cannot query '.$method.' on attribute "'.$attributeId.'" because it is an array.');
        }
    }

    /**
     * @throws \Exception
     */
    public function validateFulltextIndex(Query $query): void
    {
        if (!in_array($query->getMethod(), [Query::TYPE_SEARCH, Query::TYPE_NOT_SEARCH])) {
            return;
        }

        $collection = $this->context->getCollectionByAlias($query->getAlias());
        if ($collection->isEmpty()) {
            throw new \Exception('Unknown Alias context');
        }

        $indexes = $collection->getAttribute('indexes', []);

        foreach ($indexes as $index) {
            if (
                $index->getAttribute('type') === Database::INDEX_FULLTEXT &&
                $index->getAttribute('attributes') === [$query->getAttribute()]
            ) {
                return;
            }
        }

        throw new \Exception('Searching by attribute "'.$query->getAttribute().'" requires a fulltext index.');
    }

    /**
     * @param array<Query> $queries
     * @param string $alias
     * @return bool
     */
    public function isRelationExist(array $queries, string $alias): bool
    {
        foreach ($queries as $query) {
            if ($query->isNested()) {
                if ($this->isRelationExist($query->getValues(), $alias)) {
                    return true;
                }
            }

            if ($query->getMethod() === Query::TYPE_RELATION_EQUAL) {
                if ($query->getAlias() === $alias || $query->getRightAlias() === $alias) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Validate object attribute query values.
     *
     * Disallows ambiguous nested structures like:
     *   ['a' => [1, 'b' => [212]]]           // mixed list
     *
     * but allows:
     *   ['a' => [1, 2], 'b' => [212]]        // multiple top-level paths
     *   ['projects' => [[...]]]              // list of objects
     *   ['role' => ['name' => [...], 'ex' => [...]]]  // multiple nested paths
     *
     * @param mixed $values
     * @return bool
     */
    private function isValidObjectQueryValues(mixed $values): bool
    {
        if (!is_array($values)) {
            return true;
        }

        $hasInt = false;
        $hasString = false;

        foreach (array_keys($values) as $key) {
            if (is_int($key)) {
                $hasInt = true;
            } else {
                $hasString = true;
            }
        }

        if ($hasInt && $hasString) {
            return false;
        }

        foreach ($values as $value) {
            if (!$this->isValidObjectQueryValues($value)) {
                return false;
            }
        }

        return true;
    }
}
