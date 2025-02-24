<?php

namespace Utopia\Database\Validator\Queries;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Validator;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

class V2 extends Validator
{
    protected string $message = 'Invalid queries';

    protected array $schema = [];

    protected int $maxQueriesCount;

    private int $maxValuesCount;

    protected int $maxLimit;

    protected int $maxOffset;

    protected QueryContext $context;

    /**
     * @throws Exception
     */
    public function __construct(
        QueryContext $context,
        int $maxValuesCount = 100,
        int $maxQueriesCount = 0,
        \DateTime $minAllowedDate = new \DateTime('0000-01-01'),
        \DateTime $maxAllowedDate = new \DateTime('9999-12-31'),
        int $maxLimit = PHP_INT_MAX,
        int $maxOffset = PHP_INT_MAX
    ) {
        $this->context = $context;
        $this->maxQueriesCount = $maxQueriesCount;
        $this->maxValuesCount = $maxValuesCount;
        $this->maxLimit = $maxLimit;
        $this->maxOffset = $maxOffset;

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
                '$id' => '$internalId',
                'key' => '$internalId',
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
     * @param  array<Query|string>  $value
     *
     * @throws \Utopia\Database\Exception\Query|\Throwable
     */
    public function isValid($value): bool
    {
        try {
            if (! is_array($value)) {
                throw new \Exception('Queries must be an array');
            }

            if ($this->maxQueriesCount > 0 && \count($value) > $this->maxQueriesCount) {
                throw new \Exception('Queries count is greater than '.$this->maxQueriesCount);
            }

            foreach ($value as $query) {
                /**
                 * Removing Query::parse since we can parse in context if needed
                 */
                echo PHP_EOL.PHP_EOL.PHP_EOL.PHP_EOL;
                var_dump($query->getMethod(), $query->getCollection(), $query->getAlias());

                if ($query->isNested()) {
                    if (! self::isValid($query->getValues())) {
                        throw new \Exception($this->message);
                    }
                }

                $method = $query->getMethod();

                switch ($method) {
                    case Query::TYPE_EQUAL:
                    case Query::TYPE_CONTAINS:
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
                    case Query::TYPE_STARTS_WITH:
                    case Query::TYPE_ENDS_WITH:
                        if (count($query->getValues()) != 1) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require exactly one value.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);
                        $this->validateFulltextIndex($query);

                        break;
                    case Query::TYPE_BETWEEN:
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
                        $filters = Query::getFiltersQueries($query->getValues());

                        if (count($query->getValues()) !== count($filters)) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries can only contain filter queries');
                        }

                        if (count($filters) < 2) {
                            throw new \Exception('Invalid query: '.\ucfirst($method).' queries require at least two queries');
                        }

                        break;
                    case Query::TYPE_INNER_JOIN:
                    case Query::TYPE_LEFT_JOIN:
                    case Query::TYPE_RIGHT_JOIN:
                        var_dump('=== Query::TYPE_JOIN ===');
                        var_dump($query);
                        // validation force Query relation exist in query list!!
                        if (! self::isValid($query->getValues())) {
                            throw new \Exception($this->message);
                        }

                        break;
                    case Query::TYPE_RELATION_EQUAL:
                        var_dump('=== Query::TYPE_RELATION ===');
                        var_dump($query);
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
                        $this->validateSelect($query);

                        break;
                    case Query::TYPE_SELECTION:
                        $this->validateSelections($query);

                        break;
                    case Query::TYPE_ORDER_ASC:
                    case Query::TYPE_ORDER_DESC:
                        if (! empty($query->getAttribute())) {
                            $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        }

                        break;
                    case Query::TYPE_CURSOR_AFTER:
                    case Query::TYPE_CURSOR_BEFORE:
                        $validator = new Cursor();
                        if (! $validator->isValid($query)) {
                            throw new \Exception($validator->getDescription());
                        }

                        break;
                    default:
                        throw new \Exception('Invalid query: Method not found '.$method); // Remove this line
                        throw new \Exception('Invalid query: Method not found.');
                }
            }
        } catch (\Throwable $e) {
            $this->message = $e->getMessage();
            var_dump($e->getTraceAsString());  // Remove this line

            return false;
        }

        return true;
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
            throw new \Exception('Unknown Alias context');
        }

        if (! isset($this->schema[$collection->getId()][$attributeId])) {
            throw new \Exception('Attribute not found in schema: '.$attributeId);
        }
    }

    /**
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

        foreach ($values as $value) {

            $validator = null;

            switch ($attribute['type']) {
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
                    $validator = new DatetimeValidator();
                    break;

                case Database::VAR_RELATIONSHIP:
                    $validator = new Text(255, 0); // The query is always on uid
                    break;

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

        $array = $attribute['array'] ?? false;

        if (
            ! $array &&
            $method === Query::TYPE_CONTAINS &&
            $attribute['type'] !== Database::VAR_STRING
        ) {
            throw new \Exception('Invalid query: Cannot query contains on attribute "'.$attributeId.'" because it is not an array or string.');
        }

        if (
            $array &&
            ! in_array($method, [Query::TYPE_CONTAINS, Query::TYPE_IS_NULL, Query::TYPE_IS_NOT_NULL])
        ) {
            throw new \Exception('Invalid query: Cannot query '.$method.' on attribute "'.$attributeId.'" because it is an array.');
        }
    }

    /**
     * @throws \Exception
     */
    public function validateSelect(Query $query): void
    {
        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
        );

        foreach ($query->getValues() as $attribute) {
            $alias = Query::DEFAULT_ALIAS; // todo: Fix this
            var_dump($attribute);

            /**
             * Special symbols with `dots`
             */
            if (\str_contains($attribute, '.')) {
                try {
                    $this->validateAttributeExist($attribute, $alias);

                    continue;

                } catch (\Throwable $e) {
                    /**
                     * For relationships, just validate the top level.
                     * Will validate each nested level during the recursive calls.
                     */
                    $attribute = \explode('.', $attribute)[0];
                }
            }

            /**
             * Skip internal attributes
             */
            if (\in_array($attribute, $internalKeys)) {
                continue;
            }

            if ($attribute === '*') {
                continue;
            }

            $this->validateAttributeExist($attribute, $alias);
        }
    }

    /**
     * @throws \Exception
     */
    public function validateSelections(Query $query): void
    {
        $internalKeys = \array_map(fn ($attr) => $attr['$id'], Database::INTERNAL_ATTRIBUTES);

        $alias = $query->getAlias();
        $attribute = $query->getAttribute();

        /**
         * Special symbols with `dots`
         */
        if (\str_contains($attribute, '.')) {
            try {
                $this->validateAttributeExist($attribute, $alias);
                return;
            } catch (\Throwable $e) {
                /**
                 * For relationships, just validate the top level.
                 * Will validate each nested level during the recursive calls.
                 */
                $attribute = \explode('.', $attribute)[0];
            }
        }

        if (\in_array($attribute, $internalKeys)) {
            return;
        }

        if ($attribute === '*') {
            return;
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
}
