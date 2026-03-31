<?php

namespace Utopia\Database\Validator;

use Utopia\Database\Attribute as AttributeVO;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;
use ValueError;

/**
 * Validates database attribute definitions including type, size, format, and default values.
 */
class Attribute extends Validator
{
    protected string $message = 'Invalid attribute';

    /**
     * @var array<string, AttributeVO>
     */
    protected array $attributes = [];

    /**
     * @var array<string, AttributeVO>
     */
    protected array $schemaAttributes = [];

    /**
     * @param  array<AttributeVO|Document>  $attributes
     * @param  array<AttributeVO|Document>  $schemaAttributes
     * @param  callable|null  $attributeCountCallback
     * @param  callable|null  $attributeWidthCallback
     * @param  callable|null  $filterCallback
     */
    public function __construct(
        array $attributes,
        array $schemaAttributes = [],
        protected int $maxAttributes = 0,
        protected int $maxWidth = 0,
        protected int $maxStringLength = 0,
        protected int $maxVarcharLength = 0,
        protected int $maxIntLength = 0,
        protected bool $supportForSchemaAttributes = false,
        protected bool $supportForVectors = false,
        protected bool $supportForSpatialAttributes = false,
        protected bool $supportForObject = false,
        protected mixed $attributeCountCallback = null,
        protected mixed $attributeWidthCallback = null,
        protected mixed $filterCallback = null,
        protected bool $isMigrating = false,
        protected bool $sharedTables = false,
    ) {
        foreach ($attributes as $attribute) {
            $typed = $attribute instanceof AttributeVO ? $attribute : AttributeVO::fromDocument($attribute);
            $this->attributes[\strtolower($typed->key)] = $typed;
        }
        foreach ($schemaAttributes as $attribute) {
            $typed = $attribute instanceof AttributeVO ? $attribute : AttributeVO::fromDocument($attribute);
            $this->schemaAttributes[\strtolower($typed->key)] = $typed;
        }
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
        return false;
    }

    /**
     * Is valid.
     *
     * Returns true if attribute is valid.
     *
     * @param  AttributeVO|Document  $value
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     */
    public function isValid($value): bool
    {
        if ($value instanceof AttributeVO) {
            $attr = $value;
        } else {
            try {
                $attr = AttributeVO::fromDocument($value);
            } catch (ValueError $e) {
                /** @var string $rawType */
                $rawType = $value->getAttribute('type', 'unknown');
                $this->message = 'Unknown attribute type: '.$rawType;
                throw new DatabaseException($this->message);
            }
        }

        if (! $this->checkDuplicateId($attr)) {
            return false;
        }
        if (! $this->checkDuplicateInSchema($attr)) {
            return false;
        }
        if (! $this->checkRequiredFilters($attr)) {
            return false;
        }
        if (! $this->checkFormat($attr)) {
            return false;
        }
        if (! $this->checkAttributeLimits($attr)) {
            return false;
        }
        if (! $this->checkType($attr)) {
            return false;
        }
        if (! $this->checkDefaultValue($attr)) {
            return false;
        }

        return true;
    }

    /**
     * Check for duplicate attribute ID in collection metadata
     *
     * @throws DuplicateException
     */
    public function checkDuplicateId(AttributeVO $attribute): bool
    {
        $id = $attribute->key;

        foreach ($this->attributes as $existingAttribute) {
            if (\strtolower($existingAttribute->key) === \strtolower($id)) {
                $this->message = 'Attribute already exists in metadata';
                throw new DuplicateException($this->message);
            }
        }

        return true;
    }

    /**
     * Check for duplicate attribute ID in schema
     *
     * @throws DuplicateException
     */
    public function checkDuplicateInSchema(AttributeVO $attribute): bool
    {
        if (! $this->supportForSchemaAttributes) {
            return true;
        }

        if ($this->sharedTables && $this->isMigrating) {
            return true;
        }

        $id = $attribute->key;

        foreach ($this->schemaAttributes as $schemaAttribute) {
            /** @var string $schemaId */
            $schemaId = $this->filterCallback ? ($this->filterCallback)($schemaAttribute->key) : $schemaAttribute->key;
            if (\strtolower($schemaId) === \strtolower($id)) {
                $this->message = 'Attribute already exists in schema';
                throw new DuplicateException($this->message);
            }
        }

        return true;
    }

    /**
     * Check if required filters are present for the attribute type
     *
     * @throws DatabaseException
     */
    public function checkRequiredFilters(AttributeVO $attribute): bool
    {
        $requiredFilters = $this->getRequiredFilters($attribute->type);
        if (! empty(\array_diff($requiredFilters, $attribute->filters))) {
            $this->message = "Attribute of type: {$attribute->type->value} requires the following filters: ".implode(',', $requiredFilters);
            throw new DatabaseException($this->message);
        }

        return true;
    }

    /**
     * Get the list of required filters for each data type
     *
     * @return array<string>
     */
    protected function getRequiredFilters(ColumnType $type): array
    {
        return match ($type) {
            ColumnType::Datetime => ['datetime'],
            default => [],
        };
    }

    /**
     * Check if format is valid for the attribute type
     *
     * @throws DatabaseException
     */
    public function checkFormat(AttributeVO $attribute): bool
    {
        if ($attribute->format && ! Structure::hasFormat($attribute->format, $attribute->type)) {
            $this->message = 'Format ("'.$attribute->format.'") not available for this attribute type ("'.$attribute->type->value.'")';
            throw new DatabaseException($this->message);
        }

        return true;
    }

    /**
     * Check attribute limits (count and width)
     *
     * @throws LimitException
     */
    public function checkAttributeLimits(AttributeVO $attribute): bool
    {
        if ($this->attributeCountCallback === null || $this->attributeWidthCallback === null) {
            return true;
        }

        $attributeDoc = $attribute->toDocument();

        /** @var int $attributeCount */
        $attributeCount = ($this->attributeCountCallback)($attributeDoc);
        /** @var int $attributeWidth */
        $attributeWidth = ($this->attributeWidthCallback)($attributeDoc);

        if ($this->maxAttributes > 0 && $attributeCount > $this->maxAttributes) {
            $this->message = 'Column limit reached. Cannot create new attribute. Current attribute count is '.$attributeCount.' but the maximum is '.$this->maxAttributes.'. Remove some attributes to free up space.';
            throw new LimitException($this->message);
        }

        if ($this->maxWidth > 0 && $attributeWidth >= $this->maxWidth) {
            $this->message = 'Row width limit reached. Cannot create new attribute. Current row width is '.$attributeWidth.' bytes but the maximum is '.$this->maxWidth.' bytes. Reduce the size of existing attributes or remove some attributes to free up space.';
            throw new LimitException($this->message);
        }

        return true;
    }

    /**
     * Check attribute type and type-specific constraints
     *
     * @throws DatabaseException
     */
    public function checkType(AttributeVO $attribute): bool
    {
        $type = $attribute->type;
        $size = $attribute->size;
        $signed = $attribute->signed;
        $array = $attribute->array;
        $default = $attribute->default;

        switch ($type) {
            case ColumnType::Id:
                break;

            case ColumnType::String:
                if ($size > $this->maxStringLength) {
                    $this->message = 'Max size allowed for string is: '.number_format($this->maxStringLength);
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Varchar:
                if ($size > $this->maxVarcharLength) {
                    $this->message = 'Max size allowed for varchar is: '.number_format($this->maxVarcharLength);
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Text:
                if ($size > 65535) {
                    $this->message = 'Max size allowed for text is: 65535';
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::MediumText:
                if ($size > 16777215) {
                    $this->message = 'Max size allowed for mediumtext is: 16777215';
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::LongText:
                if ($size > 4294967295) {
                    $this->message = 'Max size allowed for longtext is: 4294967295';
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Integer:
                $limit = ($signed) ? $this->maxIntLength / 2 : $this->maxIntLength;
                if ($size > $limit) {
                    $this->message = 'Max size allowed for int is: '.number_format($limit);
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Float:
            case ColumnType::Double:
            case ColumnType::Boolean:
            case ColumnType::Datetime:
            case ColumnType::Relationship:
                break;

            case ColumnType::Object:
                if (! $this->supportForObject) {
                    $this->message = 'Object attributes are not supported';
                    throw new DatabaseException($this->message);
                }
                if (! empty($size)) {
                    $this->message = 'Size must be empty for object attributes';
                    throw new DatabaseException($this->message);
                }
                if (! empty($array)) {
                    $this->message = 'Object attributes cannot be arrays';
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Point:
            case ColumnType::Linestring:
            case ColumnType::Polygon:
                if (! $this->supportForSpatialAttributes) {
                    $this->message = 'Spatial attributes are not supported';
                    throw new DatabaseException($this->message);
                }
                if (! empty($size)) {
                    $this->message = 'Size must be empty for spatial attributes';
                    throw new DatabaseException($this->message);
                }
                if (! empty($array)) {
                    $this->message = 'Spatial attributes cannot be arrays';
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Vector:
                if (! $this->supportForVectors) {
                    $this->message = 'Vector types are not supported by the current database';
                    throw new DatabaseException($this->message);
                }
                if ($array) {
                    $this->message = 'Vector type cannot be an array';
                    throw new DatabaseException($this->message);
                }
                if ($size <= 0) {
                    $this->message = 'Vector dimensions must be a positive integer';
                    throw new DatabaseException($this->message);
                }
                if ($size > Database::MAX_VECTOR_DIMENSIONS) {
                    $this->message = 'Vector dimensions cannot exceed '.Database::MAX_VECTOR_DIMENSIONS;
                    throw new DatabaseException($this->message);
                }

                // Validate default value if provided
                if ($default !== null) {
                    if (! is_array($default)) {
                        $this->message = 'Vector default value must be an array';
                        throw new DatabaseException($this->message);
                    }
                    if (count($default) !== $size) {
                        $this->message = 'Vector default value must have exactly '.$size.' elements';
                        throw new DatabaseException($this->message);
                    }
                    foreach ($default as $component) {
                        if (! is_numeric($component)) {
                            $this->message = 'Vector default value must contain only numeric elements';
                            throw new DatabaseException($this->message);
                        }
                    }
                }
                break;

            default:
                $supportedTypes = [
                    ColumnType::String->value,
                    ColumnType::Varchar->value,
                    ColumnType::Text->value,
                    ColumnType::MediumText->value,
                    ColumnType::LongText->value,
                    ColumnType::Integer->value,
                    ColumnType::Float->value,
                    ColumnType::Double->value,
                    ColumnType::Boolean->value,
                    ColumnType::Datetime->value,
                    ColumnType::Relationship->value,
                ];
                if ($this->supportForVectors) {
                    $supportedTypes[] = ColumnType::Vector->value;
                }
                if ($this->supportForSpatialAttributes) {
                    \array_push($supportedTypes, ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value);
                }
                if ($this->supportForObject) {
                    $supportedTypes[] = ColumnType::Object->value;
                }
                $this->message = 'Unknown attribute type: '.$type->value.'. Must be one of '.implode(', ', $supportedTypes);
                throw new DatabaseException($this->message);
        }

        return true;
    }

    /**
     * Check default value constraints and type matching
     *
     * @throws DatabaseException
     */
    public function checkDefaultValue(AttributeVO $attribute): bool
    {
        $default = $attribute->default;
        $type = $attribute->type;

        if (\is_null($default)) {
            return true;
        }

        if ($attribute->required === true) {
            $this->message = 'Cannot set a default value for a required attribute';
            throw new DatabaseException($this->message);
        }

        // Reject array defaults for non-array attributes (except vectors, spatial types, and objects which use arrays internally)
        if (\is_array($default) && ! $attribute->array && ! \in_array($type, [ColumnType::Vector, ColumnType::Object, ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            $this->message = 'Cannot set an array default value for a non-array attribute';
            throw new DatabaseException($this->message);
        }

        $this->validateDefaultTypes($type, $default);

        return true;
    }

    /**
     * Function to validate if the default value of an attribute matches its attribute type
     *
     * @param  ColumnType  $type  Type of the attribute
     * @param  mixed  $default  Default value of the attribute
     *
     * @throws DatabaseException
     */
    protected function validateDefaultTypes(ColumnType $type, mixed $default): void
    {
        $defaultType = \gettype($default);

        if ($defaultType === 'NULL') {
            // Disable null. No validation required
            return;
        }

        if ($defaultType === 'array') {
            // Spatial types require the array itself
            if (! in_array($type, [ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon]) && $type !== ColumnType::Object) {
                /** @var array<mixed> $default */
                foreach ($default as $value) {
                    $this->validateDefaultTypes($type, $value);
                }
            }

            return;
        }

        switch ($type) {
            case ColumnType::String:
            case ColumnType::Varchar:
            case ColumnType::Text:
            case ColumnType::MediumText:
            case ColumnType::LongText:
                if ($defaultType !== 'string') {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.$type->value;
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Integer:
            case ColumnType::Boolean:
                if ($type->value !== $defaultType) {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.$type->value;
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Float:
            case ColumnType::Double:
                if ($defaultType !== 'double') {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.$type->value;
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Datetime:
                if ($defaultType !== 'string') {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.$type->value;
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Vector:
                // When validating individual vector components (from recursion), they should be numeric
                if ($defaultType !== 'double' && $defaultType !== 'integer') {
                    $this->message = 'Vector components must be numeric values (float or integer)';
                    throw new DatabaseException($this->message);
                }
                break;
            default:
                $supportedTypes = [
                    ColumnType::String->value,
                    ColumnType::Varchar->value,
                    ColumnType::Text->value,
                    ColumnType::MediumText->value,
                    ColumnType::LongText->value,
                    ColumnType::Integer->value,
                    ColumnType::Float->value,
                    ColumnType::Double->value,
                    ColumnType::Boolean->value,
                    ColumnType::Datetime->value,
                    ColumnType::Relationship->value,
                ];
                if ($this->supportForVectors) {
                    $supportedTypes[] = ColumnType::Vector->value;
                }
                if ($this->supportForSpatialAttributes) {
                    \array_push($supportedTypes, ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value);
                }
                $this->message = 'Unknown attribute type: '.$type->value.'. Must be one of '.implode(', ', $supportedTypes);
                throw new DatabaseException($this->message);
        }
    }
}
