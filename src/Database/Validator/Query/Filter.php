<?php

namespace Utopia\Database\Validator\Query;

use DateTime;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Sequence;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

/**
 * Validates filter query methods by checking attribute existence, type compatibility, and value constraints.
 */
class Filter extends Base
{
    /**
     * @var array<int|string, mixed>
     */
    protected array $schema = [];

    /**
     * @param  array<Document>  $attributes
     */
    public function __construct(
        array $attributes,
        private readonly string $idAttributeType,
        private readonly int $maxValuesCount = 5000,
        private readonly DateTime $minAllowedDate = new DateTime('0000-01-01'),
        private readonly DateTime $maxAllowedDate = new DateTime('9999-12-31'),
        private bool $supportForAttributes = true
    ) {
        foreach ($attributes as $attribute) {
            /** @var string $attrKey */
            $attrKey = $attribute->getAttribute('key', $attribute->getId());
            $copy = $attribute->getArrayCopy();
            // Convert type string to ColumnType enum for typed comparisons
            if (isset($copy['type']) && \is_string($copy['type'])) {
                $copy['type'] = ColumnType::from($copy['type']);
            }
            $this->schema[$attrKey] = $copy;
        }
    }

    protected function isValidAttribute(string $attribute): bool
    {
        /** @var array<string, mixed> $attributeSchema */
        $attributeSchema = $this->schema[$attribute] ?? [];
        /** @var array<string> $filters */
        $filters = $attributeSchema['filters'] ?? [];
        if (
            \in_array('encrypt', $filters)
        ) {
            $this->message = 'Cannot query encrypted attribute: '.$attribute;

            return false;
        }

        if (\str_contains($attribute, '.')) {
            // Check for special symbol `.`
            if (isset($this->schema[$attribute])) {
                return true;
            }

            // For relationships, just validate the top level.
            // will validate each nested level during the recursive calls.
            $attribute = \explode('.', $attribute)[0];
        }

        // Search for attribute in schema
        if ($this->supportForAttributes && ! isset($this->schema[$attribute])) {
            $this->message = 'Attribute not found in schema: '.$attribute;

            return false;
        }

        return true;
    }

    /**
     * @param  array<mixed>  $values
     */
    protected function isValidAttributeAndValues(string $attribute, array $values, Method $method): bool
    {
        if (! $this->isValidAttribute($attribute)) {
            return false;
        }

        $originalAttribute = $attribute;
        // isset check if for special symbols "." in the attribute name
        // same for nested path on object
        if (\str_contains($attribute, '.') && ! isset($this->schema[$attribute])) {
            // For relationships, just validate the top level.
            // Utopia will validate each nested level during the recursive calls.
            $attribute = \explode('.', $attribute)[0];
        }

        // exists and notExists queries don't require values, just attribute validation
        if (in_array($method, [Method::Exists, Method::NotExists])) {
            // Validate attribute (handles encrypted attributes, schemaless mode, etc.)
            return $this->isValidAttribute($attribute);
        }

        if (! $this->supportForAttributes && ! isset($this->schema[$attribute])) {
            // First check maxValuesCount guard for any IN-style value arrays
            if (count($values) > $this->maxValuesCount) {
                $this->message = 'Query on attribute has greater than '.$this->maxValuesCount.' values: '.$attribute;

                return false;
            }

            return true;
        }
        /** @var array<string, mixed> $attributeSchema */
        $attributeSchema = $this->schema[$attribute];

        // Skip value validation for nested relationship queries (e.g., author.age)
        // The values will be validated when querying the related collection
        /** @var ColumnType|null $schemaType */
        $schemaType = $attributeSchema['type'] ?? null;
        if ($schemaType === ColumnType::Relationship && $originalAttribute !== $attribute) {
            return true;
        }

        if (count($values) > $this->maxValuesCount) {
            $this->message = 'Query on attribute has greater than '.$this->maxValuesCount.' values: '.$attribute;

            return false;
        }

        if (! $this->supportForAttributes && ! isset($this->schema[$attribute])) {
            return true;
        }
        /** @var array<string, mixed> $attributeSchema */
        $attributeSchema = $this->schema[$attribute];

        /** @var ColumnType|null $attributeType */
        $attributeType = $attributeSchema['type'] ?? null;

        $isDottedOnObject = \str_contains($originalAttribute, '.') && $attributeType === ColumnType::Object;

        // If the query method is spatial-only, the attribute must be a spatial type
        $query = new Query($method);
        if ($query->isSpatialQuery() && ! in_array($attributeType, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            $this->message = 'Spatial query "'.$method->value.'" cannot be applied on non-spatial attribute: '.$attribute;

            return false;
        }

        foreach ($values as $value) {
            $validator = null;

            switch ($attributeType) {
                case ColumnType::Id:
                    $validator = new Sequence($this->idAttributeType, $attribute === '$sequence');
                    break;

                case ColumnType::String:
                case ColumnType::Varchar:
                case ColumnType::Text:
                case ColumnType::MediumText:
                case ColumnType::LongText:
                    $validator = new Text(0, 0);
                    break;

                case ColumnType::Integer:
                    /** @var int $size */
                    $size = $attributeSchema['size'] ?? 4;
                    /** @var bool $signed */
                    $signed = $attributeSchema['signed'] ?? true;
                    $bits = $size >= 8 ? 64 : 32;
                    // For 64-bit unsigned, use signed since PHP doesn't support true 64-bit unsigned
                    $unsigned = ! $signed && $bits < 64;
                    $validator = new Integer(false, $bits, $unsigned);
                    break;

                case ColumnType::Double:
                    $validator = new FloatValidator();
                    break;

                case ColumnType::Boolean:
                    $validator = new Boolean();
                    break;

                case ColumnType::Datetime:
                    $validator = new DatetimeValidator(
                        min: $this->minAllowedDate,
                        max: $this->maxAllowedDate
                    );
                    break;

                case ColumnType::Relationship:
                    $validator = new Text(255, 0); // The query is always on uid
                    break;

                case ColumnType::Object:
                    // For dotted attributes on objects, validate as string (path queries)
                    if ($isDottedOnObject) {
                        $validator = new Text(0, 0);
                        break;
                    }

                    // object containment queries on the base object attribute
                    elseif (\in_array($method, [Method::Equal, Method::NotEqual, Method::Contains, Method::ContainsAny, Method::ContainsAll, Method::NotContains], true)
                        && ! $this->isValidObjectQueryValues($value)) {
                        $this->message = 'Invalid object query structure for attribute "'.$attribute.'"';

                        return false;
                    }

                    continue 2;
                case ColumnType::Point:
                case ColumnType::Linestring:
                case ColumnType::Polygon:
                    if (! is_array($value)) {
                        $this->message = 'Spatial data must be an array';

                        return false;
                    }

                    continue 2;

                case ColumnType::Vector:
                    // For vector queries, validate that the value is an array of floats
                    if (! is_array($value)) {
                        $this->message = 'Vector query value must be an array';

                        return false;
                    }
                    foreach ($value as $component) {
                        if (! is_numeric($component)) {
                            $this->message = 'Vector query value must contain only numeric values';

                            return false;
                        }
                    }
                    // Check size match
                    /** @var int $expectedSize */
                    $expectedSize = $attributeSchema['size'] ?? 0;
                    if (count($value) !== $expectedSize) {
                        $this->message = "Vector query value must have {$expectedSize} elements";

                        return false;
                    }

                    continue 2;
                default:
                    $this->message = 'Unknown Data type';

                    return false;
            }

            if ($validator !== null && ! $validator->isValid($value)) {
                $this->message = 'Query value is invalid for attribute "'.$attribute.'"';

                return false;
            }
        }

        if ($attributeType === ColumnType::Relationship) {
            /**
             * We can not disable relationship query since we have logic that use it,
             * so instead we validate against the relation type
             */
            $options = $attributeSchema['options'] ?? [];

            if ($options instanceof Document) {
                $options = $options->getArrayCopy();
            }

            /** @var array<string, mixed> $options */

            /** @var string $relationTypeStr */
            $relationTypeStr = $options['relationType'] ?? '';
            /** @var bool $twoWay */
            $twoWay = $options['twoWay'] ?? false;
            /** @var string $sideStr */
            $sideStr = $options['side'] ?? '';

            $relationType = $relationTypeStr !== '' ? RelationType::from($relationTypeStr) : null;
            $side = $sideStr !== '' ? RelationSide::from($sideStr) : null;

            if ($relationType === RelationType::OneToOne && $twoWay === false && $side === RelationSide::Child) {
                $this->message = 'Cannot query on virtual relationship attribute';

                return false;
            }

            if ($relationType === RelationType::OneToMany && $side === RelationSide::Parent) {
                $this->message = 'Cannot query on virtual relationship attribute';

                return false;
            }

            if ($relationType === RelationType::ManyToOne && $side === RelationSide::Child) {
                $this->message = 'Cannot query on virtual relationship attribute';

                return false;
            }

            if ($relationType === RelationType::ManyToMany) {
                $this->message = 'Cannot query on virtual relationship attribute';

                return false;
            }
        }

        /** @var bool $array */
        $array = $attributeSchema['array'] ?? false;

        if (
            ! $array &&
            in_array($method, [Method::Contains, Method::ContainsAny, Method::ContainsAll, Method::NotContains]) &&
            ! in_array($attributeType, [ColumnType::String, ColumnType::Varchar, ColumnType::Text, ColumnType::MediumText, ColumnType::LongText]) &&
            $attributeType !== ColumnType::Object &&
            ! in_array($attributeType, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon])
        ) {
            $queryType = $method === Method::NotContains ? 'notContains' : 'contains';
            $this->message = 'Cannot query '.$queryType.' on attribute "'.$attribute.'" because it is not an array, string, or object.';

            return false;
        }

        if (
            $array &&
            ! in_array($method, [Method::Contains, Method::ContainsAny, Method::ContainsAll, Method::NotContains, Method::IsNull, Method::IsNotNull, Method::Exists, Method::NotExists])
        ) {
            $this->message = 'Cannot query '.$method->value.' on attribute "'.$attribute.'" because it is an array.';

            return false;
        }

        // Vector queries can only be used on vector attributes (not arrays)
        if (\in_array($method, [Method::VectorDot, Method::VectorCosine, Method::VectorEuclidean])) {
            if ($attributeType !== ColumnType::Vector) {
                $this->message = 'Vector queries can only be used on vector attributes';

                return false;
            }
            if ($array) {
                $this->message = 'Vector queries cannot be used on array attributes';

                return false;
            }
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
     * Validate object attribute query values.
     *
     * Disallows ambiguous nested structures like:
     *   ['a' => [1, 'b' => [212]]]           // mixed list
     *
     * but allows:
     *   ['a' => [1, 2], 'b' => [212]]        // multiple top-level paths
     *   ['projects' => [[...]]]              // list of objects
     *   ['role' => ['name' => [...], 'ex' => [...]]]  // multiple nested paths
     */
    private function isValidObjectQueryValues(mixed $values): bool
    {
        if (! is_array($values)) {
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
            if (! $this->isValidObjectQueryValues($value)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Is valid.
     *
     * Returns true if method is a filter method, attribute exists, and value matches attribute type
     *
     * Otherwise, returns false
     *
     * @param  Query  $value
     */
    public function isValid($value): bool
    {
        $method = $value->getMethod();
        $attribute = $value->getAttribute();
        switch ($method) {
            case Method::Equal:
            case Method::Contains:
            case Method::ContainsAny:
            case Method::NotContains:
            case Method::ContainsAll:
            case Method::Exists:
            case Method::NotExists:
                if ($this->isEmpty($value->getValues())) {
                    $this->message = \ucfirst($method->value).' queries require at least one value.';

                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Method::DistanceEqual:
            case Method::DistanceNotEqual:
            case Method::DistanceGreaterThan:
            case Method::DistanceLessThan:
                if (count($value->getValues()) !== 1 || ! is_array($value->getValues()[0]) || count($value->getValues()[0]) !== 3) {
                    $this->message = 'Distance query requires [[geometry, distance]] parameters';

                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Method::NotEqual:
            case Method::LessThan:
            case Method::LessThanEqual:
            case Method::GreaterThan:
            case Method::GreaterThanEqual:
            case Method::Search:
            case Method::NotSearch:
            case Method::StartsWith:
            case Method::NotStartsWith:
            case Method::EndsWith:
            case Method::NotEndsWith:
            case Method::Regex:
                if (count($value->getValues()) != 1) {
                    $this->message = \ucfirst($method->value).' queries require exactly one value.';

                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Method::Between:
            case Method::NotBetween:
                if (count($value->getValues()) != 2) {
                    $this->message = \ucfirst($method->value).' queries require exactly two values.';

                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Method::IsNull:
            case Method::IsNotNull:
                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);

            case Method::VectorDot:
            case Method::VectorCosine:
            case Method::VectorEuclidean:
                // Validate that the attribute is a vector type
                if (! $this->isValidAttribute($attribute)) {
                    return false;
                }

                // Handle dotted attributes (relationships)
                $attributeKey = $attribute;
                if (\str_contains($attributeKey, '.') && ! isset($this->schema[$attributeKey])) {
                    $attributeKey = \explode('.', $attributeKey)[0];
                }

                /** @var array<string, mixed> $attributeSchema */
                $attributeSchema = $this->schema[$attributeKey];
                /** @var ColumnType|null $vectorAttrType */
                $vectorAttrType = $attributeSchema['type'] ?? null;
                if ($vectorAttrType !== ColumnType::Vector) {
                    $this->message = 'Vector queries can only be used on vector attributes';

                    return false;
                }

                if (count($value->getValues()) != 1) {
                    $this->message = \ucfirst($method->value).' queries require exactly one vector value.';

                    return false;
                }

                return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);
            case Method::Or:
            case Method::And:
                /** @var array<Query> $andOrValues */
                $andOrValues = $value->getValues();
                $filters = Query::groupForDatabase($andOrValues)['filters'];

                if (count($value->getValues()) !== count($filters)) {
                    $this->message = \ucfirst($method->value).' queries can only contain filter queries';

                    return false;
                }

                if (count($filters) < 2) {
                    $this->message = \ucfirst($method->value).' queries require at least two queries';

                    return false;
                }

                return true;

            case Method::ElemMatch:
                // elemMatch is not supported when adapter supports attributes (schema mode)
                if ($this->supportForAttributes) {
                    $this->message = 'elemMatch is not supported by the database';

                    return false;
                }

                // Validate that the attribute (array field) exists
                if (! $this->isValidAttribute($attribute)) {
                    return false;
                }

                // For schemaless mode, allow elemMatch on any attribute
                // Validate nested queries are filter queries
                /** @var array<Query> $elemMatchValues */
                $elemMatchValues = $value->getValues();
                $filters = Query::groupForDatabase($elemMatchValues)['filters'];
                if (count($value->getValues()) !== count($filters)) {
                    $this->message = 'elemMatch queries can only contain filter queries';

                    return false;
                }

                if (count($filters) < 1) {
                    $this->message = 'elemMatch queries require at least one query';

                    return false;
                }

                return true;

            default:
                // Handle spatial query types and any other query types
                if ($value->isSpatialQuery()) {
                    if ($this->isEmpty($value->getValues())) {
                        $this->message = \ucfirst($method->value).' queries require at least one value.';

                        return false;
                    }

                    return $this->isValidAttributeAndValues($attribute, $value->getValues(), $method);
                }

                return false;
        }
    }

    /**
     * Get the maximum number of values allowed in a single filter query.
     *
     * @return int
     */
    public function getMaxValuesCount(): int
    {
        return $this->maxValuesCount;
    }

    /**
     * Get the method type this validator handles.
     *
     * @return string
     */
    public function getMethodType(): string
    {
        return self::METHOD_TYPE_FILTER;
    }
}
