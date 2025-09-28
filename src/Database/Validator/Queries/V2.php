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

    /**
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
        int $maxOffset = PHP_INT_MAX
    ) {
        $this->context = $context;
        $this->idAttributeType = $idAttributeType;
        $this->maxQueriesCount = $maxQueriesCount;
        $this->maxValuesCount = $maxValuesCount;
        $this->maxLimit = $maxLimit;
        $this->maxOffset = $maxOffset;
        $this->minAllowedDate = $minAllowedDate;
        $this->maxAllowedDate = $maxAllowedDate;

        //        $validators = [
        //            new Limit(),
        //            new Offset(),
        //            new Cursor(),
        //            new Filter($collections),
        //            new Order($collections),
        //            new Select($collections),
        //            new Join($collections),
        //        ];

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
    protected function validateAttributeExist(string $attributeId, string $alias): void
    {
        /**
         * This is for making query::select('$permissions')) pass
         */
        if($attributeId === '$permissions' || $attributeId === '$collection'){
            return;
        }

        var_dump('=== validateAttributeExist');

        //        if (\str_contains($attributeId, '.')) {
        //            // Check for special symbol `.`
        //            if (isset($this->schema[$attributeId])) {
        //                return true;
        //            }
        //
        //            // For relationships, just validate the top level.
        //            // will validate each nested level during the recursive calls.
        //            $attributeId = \explode('.', $attributeId)[0];
        //
        //            if (isset($this->schema[$attributeId])) {
        //                $this->message = 'Cannot query nested attribute on: '.$attributeId;
        //
        //                return false;
        //            }
        //        }

        $collection = $this->context->getCollectionByAlias($alias);
        if ($collection->isEmpty()) {
            throw new \Exception('Invalid query: Unknown Alias context');
        }

        if (! isset($this->schema[$collection->getId()][$attributeId])) {
            throw new \Exception('Invalid query: Attribute not found in schema: '.$attributeId);
        }
    }

    /**
     * @throws \Exception
     */
    protected function validateAlias(Query $query): void
    {
        $validator = new AliasValidator();

        if (! $validator->isValid($query->getAlias())) {
            throw new \Exception('Query '.\ucfirst($query->getMethod()).': '.$validator->getDescription());
        }

        if (! $validator->isValid($query->getRightAlias())) {
            throw new \Exception('Query '.\ucfirst($query->getMethod()).': '.$validator->getDescription());
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

        $attribute = $this->schema[$collection->getId()][$attributeId];

        $array = $attribute['array'] ?? false;
        $filters = $attribute['filters'] ?? [];

        // If the query method is spatial-only, the attribute must be a spatial type

        if (Query::isSpatialQuery($method) && !in_array($attribute['type'], Database::SPATIAL_TYPES, true)) {
            throw new \Exception('Invalid query: Spatial query "' . $method . '" cannot be applied on non-spatial attribute: ' . $attribute);
        }

        foreach ($values as $value) {
            $validator = null;

            switch ($attribute['type']) {
                case Database::VAR_ID:
                    $validator = new Sequence($this->idAttributeType, $attribute === '$sequence');
                    break;

                case Database::VAR_STRING:
                    $validator = new Text(0, 0);
                    break;

                case Database::VAR_INTEGER:
                    $validator = new Integer();
                    break;

                case Database::VAR_FLOAT:
                    $validator = new FloatValidator();
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

                case Database::VAR_POINT:
                case Database::VAR_LINESTRING:
                case Database::VAR_POLYGON:
                    if (!is_array($value)) {
                        throw new \Exception('Spatial data must be an array');
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
            !in_array($attribute['type'], Database::SPATIAL_TYPES)
        ) {
            throw new \Exception('Invalid query: Cannot query '.$method.' on attribute "'.$attributeId.'" because it is not an array or string.');
        }

        if (
            $array &&
            !in_array($method, [Query::TYPE_CONTAINS, Query::TYPE_NOT_CONTAINS, Query::TYPE_IS_NULL, Query::TYPE_IS_NOT_NULL])
        ) {
            throw new \Exception('Invalid query: Cannot query '.$method.' on attribute "'.$attributeId.'" because it is an array.');
        }

        if (Query::isFilter($method) && \in_array('encrypt', $filters)) {
            throw new \Exception('Cannot query encrypted attribute: ' . $attributeId);
        }
    }

    /**
     * @throws \Exception
     */
    public function validateSelect(Query $query): void
    {
        $asValidator = new AsValidator($query->getAttribute());
        if (! $asValidator->isValid($query->getAs())) {
            throw new \Exception('Query '.\ucfirst($query->getMethod()).': '.$asValidator->getDescription());
        }

        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
        );

        $attribute = $query->getAttribute();

        if ($attribute === '*') {
            return;
        }

        if (\in_array($attribute, $internalKeys)) {
            //return;
        }

        $alias = $query->getAlias();

        if (\str_contains($attribute, '.')) {
            if (\str_contains($attribute, '.')) {
                try {
                    /**
                     * Special symbols with `dots`
                     */
                    $this->validateAttributeExist($attribute, $alias);
                } catch (\Throwable $e) {
                    /**
                     * For relationships, just validate the top level.
                     * Will validate each nested level during the recursive calls.
                     */
                    $attribute = \explode('.', $attribute)[0];
                    $this->validateAttributeExist($attribute, $alias);
                }
            }
        }

        $this->validateAttributeExist($attribute, $alias);
    }

    /**
     * @throws \Exception
     */
    public function validateFulltextIndex(Query $query): void
    {
        if ($query->getMethod() !== Query::TYPE_SEARCH) {
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
        /**
         * Do we want to validate only top lever or nesting as well?
         */
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

            $ambiguous = [];
            $duplications = [];
            foreach ($value as $query) {
                if (!$query instanceof Query) {
                    try {
                        $query = Query::parse($query);
                    } catch (\Throwable $e) {
                        throw new \Exception('Invalid query: ' . $e->getMessage());
                    }
                }

                //var_dump($query->getMethod(), $query->getCollection(), $query->getAlias());

                $this->validateAlias($query);

                if ($query->isNested()) {
                    if (! self::isValid($query->getValues(), $scope)) {
                        throw new \Exception($this->message);
                    }
                }

                $method = $query->getMethod();

                switch ($method) {
                    case Query::TYPE_EQUAL:
                    case Query::TYPE_CONTAINS:
                    case Query::TYPE_NOT_CONTAINS:
                        if ($this->isEmpty($query->getValues())) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require at least one value.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);

                        break;
                    case Query::TYPE_CROSSES:
                    case Query::TYPE_NOT_CROSSES:
                    case Query::TYPE_DISTANCE_EQUAL:
                    case Query::TYPE_DISTANCE_NOT_EQUAL:
                    case Query::TYPE_DISTANCE_GREATER_THAN:
                    case Query::TYPE_DISTANCE_LESS_THAN:
                    case Query::TYPE_INTERSECTS:
                    case Query::TYPE_NOT_INTERSECTS:
                    case Query::TYPE_OVERLAPS:
                    case Query::TYPE_NOT_OVERLAPS:
                    case Query::TYPE_TOUCHES:
                    case Query::TYPE_NOT_TOUCHES :
                        if (count($query->getValues()) !== 1 || !is_array($query->getValues()[0]) || count($query->getValues()[0]) !== 2) {
                            $this->message = 'Distance query requires [[geometry, distance]] parameters';
                            return false;
                        }

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
                    case Query::TYPE_INNER_JOIN:
                    case Query::TYPE_LEFT_JOIN:
                    case Query::TYPE_RIGHT_JOIN:
                        $this->validateFilterQueries($query);

                        if (! self::isValid($query->getValues(), 'joins')) {
                            throw new \Exception($this->message);
                        }

                        if (! $this->isRelationExist($query->getValues(), $query->getAlias())) {
                            throw new \Exception('Invalid query: At least one relation query is required on the joined collection.');
                        }

                        /**
                         * todo:to all queries which uses aliases check that it is available in context scope, not just exists
                         */
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
                        $validator = new AsValidator($query->getAttribute());

                        if (! $validator->isValid($query->getAs())) {
                            throw new \Exception('Invalid Query Select: '.$validator->getDescription());
                        }

                        $this->validateSelect($query);

                        if($query->getAttribute() === '*'){
                            $collection = $this->context->getCollectionByAlias($query->getAlias());
                            $attributes = $this->schema[$collection->getId()];
                            foreach ($attributes as $attribute){
                                if (($duplications[$query->getAlias()][$attribute['$id']] ?? false) === true){
                                    //throw new \Exception('Ambiguous column using "*" for "'.$query->getAlias().'.'.$attribute['$id'].'"');
                                }

                                $duplications[$query->getAlias()][$attribute['$id']] = true;
                            }
                        } else {
                            if (($duplications[$query->getAlias()][$query->getAttribute()] ?? false) === true){
                                //throw new \Exception('Duplicate Query Select on "'.$query->getAlias().'.'.$query->getAttribute().'"');
                            }
                            $duplications[$query->getAlias()][$query->getAttribute()] = true;
                        }

                        if (!empty($query->getAs())){
                            $needle = $query->getAs();
                        } else {
                            $needle = $query->getAttribute(); // todo: convert internal attribute from $id => _id
                        }

                        if (in_array($needle, $ambiguous)){
                            //throw new \Exception('Invalid Query Select: ambiguous column "'.$needle.'"');
                        }

                        $ambiguous[] = $needle;

                        break;

                    case Query::TYPE_ORDER_RANDOM:
                        /**
                         * todo: Validations
                         */
                        break;
                    case Query::TYPE_ORDER_ASC:
                    case Query::TYPE_ORDER_DESC:
                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());

                        break;
                    case Query::TYPE_CURSOR_AFTER:
                    case Query::TYPE_CURSOR_BEFORE:
                        $validator = new Cursor();
                        if (! $validator->isValid($query)) {
                            throw new \Exception($validator->getDescription());
                        }

                        break;

                    default:
                        throw new \Exception('Invalid query: Method not found ');
                }
            }

        } catch (\Throwable $e) {
            $this->message = $e->getMessage();
            var_dump($this->message);
            var_dump($e);

            return false;
        }

        return true;
    }
}
