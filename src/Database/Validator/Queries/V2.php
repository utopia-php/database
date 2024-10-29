<?php

namespace Utopia\Database\Validator\Queries;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Query\Base;
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
use Utopia\Validator\Text;

class V2 extends Validator
{
    protected string $message = 'Invalid queries';

    protected array $collections = [];

    protected array $schema = [];

    protected int $length;

    private int $maxValuesCount;

    /**
     * Expression constructor
     *
     * @param array<Document> $collections
     * @throws Exception
     */
    public function __construct(array $collections, int $length = 0, int $maxValuesCount = 100)
    {
        foreach ($collections as $collection) {
            $this->collections[$collection->getId()] = $collection->getArrayCopy();

            $attributes = $collection->getAttribute('attributes', []);
            foreach ($attributes as $attribute) {
                // todo: Add internal id's?
                $this->schema[$collection->getId()][$attribute->getAttribute('key', $attribute->getAttribute('$id'))] = $attribute->getArrayCopy();
            }
        }

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
     * @param array<Query|string> $value
     * @return bool
     * @throws \Utopia\Database\Exception\Query
     */
    public function isValid($value): bool
    {
        if (!is_array($value)) {
            $this->message = 'Queries must be an array';
            return false;
        }

        if ($this->length && \count($value) > $this->length) {
            return false;
        }

        var_dump("ininininininininininininininin");

        foreach ($value as $query) {
            if (!$query instanceof Query) {
                try {
                    $query = Query::parse($query);
                } catch (\Throwable $e) {
                    $this->message = 'Invalid query: ' . $e->getMessage();
                    return false;
                }
            }

            if($query->isNested()) {
                if(!self::isValid($query->getValues())) {
                    return false;
                }
            }

            $method = $query->getMethod();
            $attribute = $query->getAttribute();

            switch ($method) {
                case Query::TYPE_EQUAL:
                case Query::TYPE_CONTAINS:
                    if ($this->isEmpty($query->getValues())) {
                        $this->message = \ucfirst($method) . ' queries require at least one value.';
                        return false;
                    }

                    return $this->isValidAttributeAndValues($attribute, $query->getValues(), $method);

                case Query::TYPE_NOT_EQUAL:
                case Query::TYPE_LESSER:
                case Query::TYPE_LESSER_EQUAL:
                case Query::TYPE_GREATER:
                case Query::TYPE_GREATER_EQUAL:
                case Query::TYPE_SEARCH:
                case Query::TYPE_STARTS_WITH:
                case Query::TYPE_ENDS_WITH:
                    if (count($query->getValues()) != 1) {
                        $this->message = \ucfirst($method) . ' queries require exactly one value.';
                        return false;
                    }

                    return $this->isValidAttributeAndValues($attribute, $query->getValues(), $method);

                case Query::TYPE_BETWEEN:
                    if (count($query->getValues()) != 2) {
                        $this->message = \ucfirst($method) . ' queries require exactly two values.';
                        return false;
                    }

                    return $this->isValidAttributeAndValues($attribute, $query->getValues(), $method);

                case Query::TYPE_IS_NULL:
                case Query::TYPE_IS_NOT_NULL:
                    return $this->isValidAttributeAndValues($attribute, $query->getValues(), $method);

                case Query::TYPE_OR:
                case Query::TYPE_AND:
                    $filters = Query::groupByType($query->getValues())['filters'];

                    if(count($query->getValues()) !== count($filters)) {
                        $this->message = \ucfirst($method) . ' queries can only contain filter queries';
                        return false;
                    }

                    if(count($filters) < 2) {
                        $this->message = \ucfirst($method) . ' queries require at least two queries';
                        return false;
                    }

                    return true;

                case Query::TYPE_RELATION:
                    echo "Hello TYPE_RELATION";
                    break;

                default:
                    return false;
            }
        }

        return false;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription(): string
    {
        return $this->message;
    }

    /**
     * Is array
     *
     * Function will return true if object is array.
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return true;
    }

    /**
     * Get Type
     *
     * Returns validator type.
     *
     * @return string
     */
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    /**
     * @param array<mixed> $values
     * @return bool
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
     * @param string $attribute
     * @return bool
     */
    protected function isValidAttribute(string $attribute): bool
    {
        if (\str_contains($attribute, '.')) {
            // Check for special symbol `.`
            if (isset($this->schema[$attribute])) {
                return true;
            }

            // For relationships, just validate the top level.
            // will validate each nested level during the recursive calls.
            $attribute = \explode('.', $attribute)[0];

            if (isset($this->schema[$attribute])) {
                $this->message = 'Cannot query nested attribute on: ' . $attribute;
                return false;
            }
        }

        // Search for attribute in schema
        if (!isset($this->schema[$attribute])) {
            $this->message = 'Attribute not found in schema: ' . $attribute;
            return false;
        }

        return true;
    }

    /**
     * @param string $attribute
     * @param array<mixed> $values
     * @return bool
     */
    protected function isValidAttributeAndValues(string $attribute, array $values, string $method): bool
    {
        if (!$this->isValidAttribute($attribute)) {
            return false;
        }

        // isset check if for special symbols "." in the attribute name
        if (\str_contains($attribute, '.') && !isset($this->schema[$attribute])) {
            // For relationships, just validate the top level.
            // Utopia will validate each nested level during the recursive calls.
            $attribute = \explode('.', $attribute)[0];
        }

        $attributeSchema = $this->schema[$attribute];

        if (count($values) > $this->maxValuesCount) {
            $this->message = 'Query on attribute has greater than ' . $this->maxValuesCount . ' values: ' . $attribute;
            return false;
        }

        // Extract the type of desired attribute from collection $schema
        $attributeType = $attributeSchema['type'];

        foreach ($values as $value) {

            $validator = null;

            switch ($attributeType) {
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
                    $this->message = 'Unknown Data type';
                    return false;
            }

            if (!$validator->isValid($value)) {
                $this->message = 'Query value is invalid for attribute "' . $attribute . '"';
                return false;
            }
        }

        if($attributeSchema['type'] === 'relationship') {
            /**
             * We can not disable relationship query since we have logic that use it,
             * so instead we validate against the relation type
             */
            $options = $attributeSchema['options'];

            if($options['relationType'] === Database::RELATION_ONE_TO_ONE && $options['twoWay'] === false && $options['side'] === Database::RELATION_SIDE_CHILD) {
                $this->message = 'Cannot query on virtual relationship attribute';
                return false;
            }

            if($options['relationType'] === Database::RELATION_ONE_TO_MANY && $options['side'] === Database::RELATION_SIDE_PARENT) {
                $this->message = 'Cannot query on virtual relationship attribute';
                return false;
            }

            if($options['relationType'] === Database::RELATION_MANY_TO_ONE && $options['side'] === Database::RELATION_SIDE_CHILD) {
                $this->message = 'Cannot query on virtual relationship attribute';
                return false;
            }

            if($options['relationType'] === Database::RELATION_MANY_TO_MANY) {
                $this->message = 'Cannot query on virtual relationship attribute';
                return false;
            }
        }

        $array = $attributeSchema['array'] ?? false;

        if(
            !$array &&
            $method === Query::TYPE_CONTAINS &&
            $attributeSchema['type'] !==  Database::VAR_STRING
        ) {
            $this->message = 'Cannot query contains on attribute "' . $attribute . '" because it is not an array or string.';
            return false;
        }

        if(
            $array &&
            !in_array($method, [Query::TYPE_CONTAINS, Query::TYPE_IS_NULL, Query::TYPE_IS_NOT_NULL])
        ) {
            $this->message = 'Cannot query '. $method .' on attribute "' . $attribute . '" because it is an array.';
            return false;
        }

        return true;
    }
}
