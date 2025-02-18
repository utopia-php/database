<?php

namespace Utopia\Database\Validator\Queries;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Limit;
use Utopia\Database\Validator\Query\Offset;
use Utopia\Database\Validator\Query\Order;
use Utopia\Database\Validator\Query\Select;
use Utopia\Validator;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Numeric;
use Utopia\Validator\Range;
use Utopia\Validator\Text;

class V2 extends Validator
{
    protected string $message = 'Invalid queries';

    // protected string $collectionId = '';

    // protected array $collections = [];

    protected array $schema = [];

    protected int $length;

    private int $maxValuesCount;

    protected int $maxLimit;

    protected int $maxOffset;

    private array $aliases = [];

    /**
     * Expression constructor
     */
    public function __construct(QueryContext $context, int $length = 0, int $maxValuesCount = 100, int $maxLimit = PHP_INT_MAX, int $maxOffset = PHP_INT_MAX)
    {
        $collections = $context->getCollections();

        foreach ($collections as $i => $collection) {
            if ($i === 0) {
                $this->aliases[''] = $collection->getId();
            }

            // $this->collections[$collection->getId()] = $collection->getArrayCopy();

            $attributes = $collection->getAttribute('attributes', []);
            foreach ($attributes as $attribute) {
                // todo: internal id's?
                $this->schema[$collection->getId()][$attribute->getAttribute('key', $attribute->getAttribute('$id'))] = $attribute->getArrayCopy();
            }
        }

        $this->maxLimit = $maxLimit;
        $this->maxOffset = $maxOffset;
        $this->length = $length;
        $this->maxValuesCount = $maxValuesCount;

        //        $attributes[] = new Document([
        //            '$id' => '$id',
        //            'key' => '$id',
        //            'type' => Database::VAR_STRING,
        //            'array' => false,
        //        ]);
        //        $attributes[] = new Document([
        //            '$id' => '$internalId',
        //            'key' => '$internalId',
        //            'type' => Database::VAR_STRING,
        //            'array' => false,
        //        ]);
        //        $attributes[] = new Document([
        //            '$id' => '$createdAt',
        //            'key' => '$createdAt',
        //            'type' => Database::VAR_DATETIME,
        //            'array' => false,
        //        ]);
        //        $attributes[] = new Document([
        //            '$id' => '$updatedAt',
        //            'key' => '$updatedAt',
        //            'type' => Database::VAR_DATETIME,
        //            'array' => false,
        //        ]);

        //        $validators = [
        //            new Limit(),
        //            new Offset(),
        //            new Cursor(),
        //            new Filter($collections),
        //            new Order($collections),
        //            new Select($collections),
        //            new Join($collections),
        //        ];
    }

    /**
     * @param  array<Query|string>  $value
     *
     * @throws \Utopia\Database\Exception\Query
     */
    public function isValid($value): bool
    {
        try {
            if (! is_array($value)) {
                throw new \Exception('Queries must be an array');
            }

            if ($this->length && \count($value) > $this->length) {
                throw new \Exception('Queries count is greater than '.$this->length);
            }

            $queries = [];

            foreach ($value as $query) {
                if (! $query instanceof Query) {
                    try {
                        $query = Query::parse($query);
                    } catch (\Throwable $e) {
                        throw new \Exception('Invalid query: '.$e->getMessage());
                    }
                }

                if ($query->getMethod() === Query::TYPE_JOIN) {
                    var_dump($query);
                    //$this->aliases[$query->getAlias()] = $query->getCollection();
                }

                $queries[] = $query;
            }

            foreach ($queries as $query) {
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
                            throw new \Exception(\ucfirst($method).' queries require at least one value.');
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
                            throw new \Exception(\ucfirst($method).' queries require exactly one value.');
                        }

                        $this->validateAttributeExist($query->getAttribute(), $query->getAlias());
                        $this->validateValues($query->getAttribute(), $query->getAlias(), $query->getValues(), $method);

                        break;

                    case Query::TYPE_BETWEEN:
                        if (count($query->getValues()) != 2) {
                            throw new \Exception(\ucfirst($method).' queries require exactly two values.');
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
                        $filters = Query::groupByType($query->getValues())['filters'];

                        if (count($query->getValues()) !== count($filters)) {
                            throw new \Exception(\ucfirst($method).' queries can only contain filter queries');
                        }

                        if (count($filters) < 2) {
                            throw new \Exception(\ucfirst($method).' queries require at least two queries');
                        }

                        break;

                    case Query::TYPE_RELATION:
                        var_dump('=== Query::TYPE_RELATION ===');

                        break;

                    case Query::TYPE_JOIN:
                        var_dump('=== Query::TYPE_JOIN ===');

                        break;

                    case Query::TYPE_LIMIT:
                        $this->validateLimit($query);

                        break;

                    case Query::TYPE_OFFSET:
                        $this->validateOffset($query);

                        break;

                    case Query::TYPE_SELECT:
                        $this->validateSelect($query);

                        break;

                    default:
                        throw new \Exception('Invalid query: Method not found '.$method); // Remove this line
                        throw new \Exception('Invalid query: Method not found.');
                }
            }
        } catch (\Throwable $e) {
            $this->message = $e->getMessage();
            throw $e; // Remove this!

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

        $collectionId = $this->aliases[$alias];
        var_dump('=== attribute === '.$attributeId);
        var_dump('=== alias === '.$alias);
        var_dump('=== collectionId === '.$collectionId);

        var_dump($this->schema[$collectionId][$attributeId]);

        if (! isset($this->schema[$collectionId][$attributeId])) {
            throw new \Exception('Attribute not found in schema: '.$attributeId);
        }
    }

    protected function validateValues(string $attributeId, string $alias, array $values, string $method): void
    {
        if (count($values) > $this->maxValuesCount) {
            throw new \Exception('Query on attribute has greater than '.$this->maxValuesCount.' values: '.$attributeId);
        }

        $collectionId = $this->aliases[$alias];

        $attribute = $this->schema[$collectionId][$attributeId];

        foreach ($values as $value) {

            $validator = null;

            switch ($attribute['type']) {
                case Database::VAR_STRING:
                    $validator = new Text(0, 0);
                    break;

                case Database::VAR_INTEGER:
                    $validator = new Integer;
                    break;

                case Database::VAR_FLOAT:
                    $validator = new FloatValidator;
                    break;

                case Database::VAR_BOOLEAN:
                    $validator = new Boolean;
                    break;

                case Database::VAR_DATETIME:
                    $validator = new DatetimeValidator;
                    break;

                case Database::VAR_RELATIONSHIP:
                    $validator = new Text(255, 0); // The query is always on uid
                    break;

                default:
                    throw new \Exception('Unknown Data type');
            }

            if (! $validator->isValid($value)) {
                throw new \Exception('Query value is invalid for attribute "'.$attributeId.'"');
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
            throw new \Exception('Cannot query contains on attribute "'.$attributeId.'" because it is not an array or string.');
        }

        if (
            $array &&
            ! in_array($method, [Query::TYPE_CONTAINS, Query::TYPE_IS_NULL, Query::TYPE_IS_NOT_NULL])
        ) {
            throw new \Exception('Cannot query '.$method.' on attribute "'.$attributeId.'" because it is an array.');
        }
    }

    public function validateLimit(Query $query): void
    {
        $limit = $query->getValue();

        $validator = new Numeric;
        if (! $validator->isValid($limit)) {
            throw new \Exception('Invalid limit: '.$validator->getDescription());
        }

        $validator = new Range(1, $this->maxLimit);
        if (! $validator->isValid($limit)) {
            throw new \Exception('Invalid limit: '.$validator->getDescription());
        }
    }

    public function validateOffset(Query $query): void
    {
        $offset = $query->getValue();

        $validator = new Numeric;
        if (! $validator->isValid($offset)) {
            throw new \Exception('Invalid limit: '.$validator->getDescription());
        }

        $validator = new Range(0, $this->maxOffset);
        if (! $validator->isValid($offset)) {
            throw new \Exception('Invalid offset: '.$validator->getDescription());
        }
    }

    public function validateSelect(Query $query): void
    {
        $internalKeys = \array_map(
            fn ($attr) => $attr['$id'],
            Database::INTERNAL_ATTRIBUTES
        );

        foreach ($query->getValues() as $attribute) {

            if (is_string()) {

            } elseif ($this->isArray()) {

            }

            if ($this->validateAttributeExist()) {

            }

            //            if (\str_contains($attribute, '.')) {
            //                //special symbols with `dots`
            //                if (isset($this->schema[$attribute])) {
            //                    continue;
            //                }
            //
            //                // For relationships, just validate the top level.
            //                // Will validate each nested level during the recursive calls.
            //                $attribute = \explode('.', $attribute)[0];
            //            }

            if (\in_array($attribute, $internalKeys)) {
                continue;
            }

            if (! isset($this->schema[$attribute]) && $attribute !== '*') {
                throw new \Exception('Attribute not found in schema: '.$attribute);
            }
        }
    }
}
