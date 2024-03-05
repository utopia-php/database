<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

class Filter extends Base
{
    /**
     * @var array<int|string, mixed>
     */
    protected array $schema = [];

    private int $maxValuesCount;

    /**
     * @param array<Document> $attributes
     * @param int $maxValuesCount
     */
    public function __construct(array $attributes = [], int $maxValuesCount = 100)
    {
        foreach ($attributes as $attribute) {
            $this->schema[$attribute->getAttribute('key', $attribute->getAttribute('$id'))] = $attribute->getArrayCopy();
        }

        $this->maxValuesCount = $maxValuesCount;
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
                    break;
                default:
                    $this->message = 'Unknown Data type';
                    return false;
            }

            if (!\is_null($validator) && !$validator->isValid($value)) {
                $this->message = 'Query value is invalid for attribute "' . $attribute . '"';
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
     * Is valid.
     *
     * Returns true if method is a filter method, attribute exists, and value matches attribute type
     *
     * Otherwise, returns false
     *
     * @param Query $value
     * @return bool
     */
    public function isValid($value): bool
    {
        $method = $value->getMethod();
        $attribute = $value->getAttribute();
        switch ($method) {
            case Query::TYPE_EQUAL:
            case Query::TYPE_CONTAINS:
                if ($this->isEmpty($value->getValues())) {
                    $this->message = \ucfirst($method) . ' queries require at least one value.';
                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Query::TYPE_NOT_EQUAL:
            case Query::TYPE_LESSER:
            case Query::TYPE_LESSER_EQUAL:
            case Query::TYPE_GREATER:
            case Query::TYPE_GREATER_EQUAL:
            case Query::TYPE_SEARCH:
            case Query::TYPE_STARTS_WITH:
            case Query::TYPE_ENDS_WITH:
                if (count($value->getValues()) != 1) {
                    $this->message = \ucfirst($method) . ' queries require exactly one value.';
                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Query::TYPE_BETWEEN:
                if (count($value->getValues()) != 2) {
                    $this->message = \ucfirst($method) . ' queries require exactly two values.';
                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Query::TYPE_IS_NULL:
            case Query::TYPE_IS_NOT_NULL:
                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Query::TYPE_OR:
            case Query::TYPE_AND:
                $filters = Query::groupByType($value->getValues())['filters'];

                if(count($value->getValues()) !== count($filters)) {
                    $this->message = \ucfirst($method) . ' queries can only contain filter queries';
                    return false;
                }

                if(count($filters) < 2) {
                    $this->message = \ucfirst($method) . ' queries require at least two queries';
                    return false;
                }

                return true;

            default:
                return false;
        }
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_FILTER;
    }
}
