<?php
//
//namespace Utopia\Database\Validator;
//
//use Utopia\Database\Database;
//use Utopia\Validator;
//use Utopia\Validator\Range;
//use Utopia\Database\Document;
//use Utopia\Database\Query as DatabaseQuery;
//
//class Query extends Validator
//{
//    /**
//     * @var string
//     */
//    protected string $message = 'Invalid query';
//
//    /**
//     * @var array<string, array<string, mixed>>
//     */
//    protected array $schema = [];
//
//    protected int $maxLimit;
//    protected int $maxOffset;
//    protected int $maxValuesCount;
//
//    /**
//     * Query constructor
//     *
//     * @param array<Document> $attributes
//     * @param int $maxLimit
//     * @param int $maxOffset
//     * @param int $maxValuesCount
//     */
//    public function __construct(array $attributes, int $maxLimit = 100, int $maxOffset = 5000, int $maxValuesCount = 100)
//    {
//        $this->schema['$id'] = [
//            'key' => '$id',
//            'array' => false,
//            'type' => Database::VAR_STRING,
//            'size' => 512
//        ];
//
//        $this->schema['$createdAt'] = [
//            'key' => '$createdAt',
//            'array' => false,
//            'type' => Database::VAR_DATETIME,
//            'size' => 0
//        ];
//
//        $this->schema['$updatedAt'] = [
//            'key' => '$updatedAt',
//            'array' => false,
//            'type' => Database::VAR_DATETIME,
//            'size' => 0
//        ];
//
//        foreach ($attributes as $attribute) {
//            $this->schema[(string)$attribute->getAttribute('key')] = $attribute->getArrayCopy();
//        }
//
//        $this->maxLimit = $maxLimit;
//        $this->maxOffset = $maxOffset;
//        $this->maxValuesCount = $maxValuesCount;
//    }
//
//    /**
//     * Get Description.
//     *
//     * Returns validator description
//     *
//     * @return string
//     */
//    public function getDescription(): string
//    {
//        return $this->message;
//    }
//
//    protected function isValidLimit(?int $limit): bool
//    {
//        $validator = new Range(0, $this->maxLimit);
//        if ($validator->isValid($limit)) {
//            return true;
//        }
//
//        $this->message = 'Invalid limit: ' . $validator->getDescription();
//        return false;
//    }
//
//    protected function isValidOffset(?int $offset): bool
//    {
//        $validator = new Range(0, $this->maxOffset);
//        if ($validator->isValid($offset)) {
//            return true;
//        }
//
//        $this->message = 'Invalid offset: ' . $validator->getDescription();
//        return false;
//    }
//
//    protected function isValidCursor(?string $cursor): bool
//    {
//        if ($cursor === null) {
//            $this->message = 'Cursor must not be null';
//            return false;
//        }
//        return true;
//    }
//
//    protected function isValidAttribute(string $attribute): bool
//    {
//        // Search for attribute in schema
//        if (!isset($this->schema[$attribute])) {
//            $this->message = 'Attribute not found in schema: ' . $attribute;
//            return false;
//        }
//
//        return true;
//    }
//
//    /**
//     * @param string $attribute
//     * @param array<mixed> $values
//     * @return bool
//     */
//    protected function isValidAttributeAndValues(string $attribute, array $values): bool
//    {
//        if (!$this->isValidAttribute($attribute)) {
//            return false;
//        }
//
//        $attributeSchema = $this->schema[$attribute];
//
//        if (count($values) > $this->maxValuesCount) {
//            $this->message = 'Query on attribute has greater than ' . $this->maxValuesCount . ' values: ' . $attribute;
//            return false;
//        }
//
//        // Extract the type of desired attribute from collection $schema
//        $attributeType = $attributeSchema['type'];
//
//        foreach ($values as $value) {
//            switch ($attributeType) {
//                case Database::VAR_DATETIME:
//                    $condition = gettype($value) === Database::VAR_STRING;
//                    break;
//                case Database::VAR_FLOAT:
//                    $condition = (gettype($value) === Database::VAR_FLOAT || gettype($value) === Database::VAR_INTEGER);
//                    break;
//                default:
//                    $condition = gettype($value) === $attributeType;
//                    break;
//            }
//
//            if (!$condition) {
//                $this->message = 'Query type does not match expected: ' . $attributeType;
//                return false;
//            }
//        }
//
//        return true;
//    }
//
//    /**
//     * @param string $attribute
//     * @param array<mixed> $values
//     * @return bool
//     */
//    protected function isValidContains(string $attribute, array $values): bool
//    {
//        if (!$this->isValidAttributeAndValues($attribute, $values)) {
//            return false;
//        }
//
//        $attributeSchema = $this->schema[$attribute];
//
//        // Contains method only supports array attributes
//        if (!$attributeSchema['array']) {
//            $this->message = 'Query method only supported on array attributes: ' . DatabaseQuery::TYPE_CONTAINS;
//            return false;
//        }
//
//        return true;
//    }
//
//    /**
//     * @param array<string> $attributes
//     * @return bool
//     */
//    protected function isValidSelect(array $attributes): bool
//    {
//        foreach ($attributes as $attribute) {
//            if (!$this->isValidAttribute($attribute)) {
//                return false;
//            }
//        }
//
//        return true;
//    }
//
//    /**
//     * Is valid.
//     *
//     * Returns false if:
//     * 1. $query has an invalid method
//     * 2. limit value is not a number, less than 0, or greater than $maxLimit
//     * 3. offset value is not a number, less than 0, or greater than $maxOffset
//     * 4. attribute does not exist
//     * 5. count of values is greater than $maxValuesCount
//     * 6. value type does not match attribute type
//     * 6. contains method is used on non-array attribute
//     *
//     * Otherwise, returns true.
//     *
//     * @param DatabaseQuery $query
//     *
//     * @return bool
//     */
//    public function isValid($query): bool
//    {
//        // Validate method
//        $method = $query->getMethod();
//        if (!DatabaseQuery::isMethod($method)) {
//            $this->message = 'Query method invalid: ' . $method;
//            return false;
//        }
//
//        $attribute = $query->getAttribute();
//
//        switch ($method) {
//            case DatabaseQuery::TYPE_LIMIT:
//                $limit = $query->getValue();
//                return $this->isValidLimit($limit);
//
//            case DatabaseQuery::TYPE_OFFSET:
//                $offset = $query->getValue();
//                return $this->isValidOffset($offset);
//
//            case DatabaseQuery::TYPE_CURSORAFTER:
//            case DatabaseQuery::TYPE_CURSORBEFORE:
//                $cursor = $query->getValue();
//                return $this->isValidCursor($cursor);
//
//            case DatabaseQuery::TYPE_ORDERASC:
//            case DatabaseQuery::TYPE_ORDERDESC:
//                // Allow empty string for order attribute so we can order by natural order
//                if ($attribute === '') {
//                    return true;
//                }
//                return $this->isValidAttribute($attribute);
//
//            case DatabaseQuery::TYPE_CONTAINS:
//                $values = $query->getValues();
//                return $this->isValidContains($attribute, $values);
//
//            case DatabaseQuery::TYPE_SELECT:
//                $attributes = $query->getValues();
//                return $this->isValidSelect($attributes);
//
//            default:
//                // other filter queries
//                $values = $query->getValues();
//                return $this->isValidAttributeAndValues($attribute, $values);
//        }
//    }
//    /**
//     * Is array
//     *
//     * Function will return true if object is array.
//     *
//     * @return bool
//     */
//    public function isArray(): bool
//    {
//        return false;
//    }
//
//    /**
//     * Get Type
//     *
//     * Returns validator type.
//     *
//     * @return string
//     */
//    public function getType(): string
//    {
//        return self::TYPE_OBJECT;
//    }
//}
