<?php

namespace Utopia\Database\Validator;

use Closure;
use stdClass;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Schema\Column;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;

/**
 * Validates database attribute definitions including type, size, format, and default values.
 */
class AttributeDefinition extends Validator
{
    private const string JSON_FILTER = 'json';

    private const array STRING_TYPES = [
        ColumnType::String,
        ColumnType::Varchar,
        ColumnType::Text,
        ColumnType::MediumText,
        ColumnType::LongText,
    ];

    private const array SPATIAL_TYPES = [
        ColumnType::Point,
        ColumnType::Linestring,
        ColumnType::Polygon,
    ];

    protected string $message = 'Invalid attribute';

    /**
     * @var array<string, Attribute>
     */
    protected array $attributes = [];

    /**
     * @var list<string>
     */
    protected array $schemaAttributes = [];

    /**
     * Schema attributes are the engine's physical columns, read only for their names.
     *
     * @param  array<Attribute|Document>  $attributes
     * @param  list<Column>  $schemaAttributes
     * @param  (Closure(Document): int)|null  $attributeCount
     * @param  (Closure(Document): int)|null  $attributeWidth
     * @param  (Closure(string): string)|null  $filter
     */
    public function __construct(
        array $attributes,
        protected readonly Profile $profile,
        array $schemaAttributes = [],
        protected readonly ?Closure $attributeCount = null,
        protected readonly ?Closure $attributeWidth = null,
        protected readonly ?Closure $filter = null,
    ) {
        foreach ($attributes as $attribute) {
            $typed = $attribute instanceof Attribute ? $attribute : Attribute::fromDocument($attribute);
            $this->attributes[\strtolower($typed->key)] = $typed;
        }
        foreach ($schemaAttributes as $column) {
            $this->schemaAttributes[] = $column->name;
        }
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_OBJECT;
    }

    #[\Override]
    public function getDescription(): string
    {
        return $this->message;
    }

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    /**
     * Returns true if attribute is valid.
     *
     * @param  mixed  $value
     *
     * @throws DatabaseException
     * @throws DuplicateException
     * @throws LimitException
     */
    #[\Override]
    public function isValid(mixed $value): bool
    {
        if ($value instanceof Document) {
            try {
                $value = Attribute::fromDocument($value);
            } catch (StructureException $error) {
                $this->message = $error->getMessage();
                throw new DatabaseException($this->message, previous: $error);
            }
        }

        if (! $value instanceof Attribute) {
            $this->message = 'Value must be an attribute';

            return false;
        }

        if (! $this->checkDuplicateId($value)) {
            return false;
        }
        if (! $this->checkDuplicateInSchema($value)) {
            return false;
        }
        if (! $this->checkRequiredFilters($value)) {
            return false;
        }
        if (! $this->checkFormat($value)) {
            return false;
        }
        if (! $this->checkType($value)) {
            return false;
        }
        if (! $this->checkAttributeLimits($value)) {
            return false;
        }
        if (! $this->checkDefaultValue($value)) {
            return false;
        }

        return true;
    }

    /**
     * Check for duplicate attribute ID in collection metadata
     *
     * @throws DuplicateException
     */
    public function checkDuplicateId(Attribute $attribute): bool
    {
        $id = \strtolower($attribute->key);

        foreach ($this->attributes as $existingAttribute) {
            if (\strtolower($existingAttribute->key) === $id) {
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
    public function checkDuplicateInSchema(Attribute $attribute): bool
    {
        if (! $this->profile->supports(Capability::SchemaIntrospection)) {
            return true;
        }

        if ($this->profile->sharedTables && $this->profile->migrating) {
            return true;
        }

        $id = \strtolower($attribute->key);

        foreach ($this->schemaAttributes as $schemaAttribute) {
            $schemaId = $this->filter === null ? $schemaAttribute : ($this->filter)($schemaAttribute);
            if (\strtolower($schemaId) === $id) {
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
    public function checkRequiredFilters(Attribute $attribute): bool
    {
        $requiredFilters = $this->getRequiredFilters($attribute->type);
        if (! empty(\array_diff($requiredFilters, $attribute->filters))) {
            $this->message = 'Attribute of type: '.$attribute->type->value.' requires the following filters: '.implode(',', $requiredFilters);
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
    public function checkFormat(Attribute $attribute): bool
    {
        $format = $attribute->format?->name;
        if ($format && ! Structure::hasFormat($format, $attribute->type)) {
            $this->message = 'Format ("'.$format.'") not available for this attribute type ("'.$attribute->type->value.'")';
            throw new DatabaseException($this->message);
        }

        return true;
    }

    /**
     * Check attribute limits (count and width)
     *
     * @throws LimitException
     */
    public function checkAttributeLimits(Attribute $attribute): bool
    {
        if ($this->attributeCount === null || $this->attributeWidth === null) {
            return true;
        }

        $document = $attribute->toDocument();
        $attributeCount = ($this->attributeCount)($document);
        $attributeWidth = ($this->attributeWidth)($document);
        $maxAttributes = $this->profile->limits->attributes;
        $maxWidth = $this->profile->limits->documentSize;

        if ($maxAttributes > 0 && $attributeCount > $maxAttributes) {
            $this->message = 'Column limit reached. Cannot create new attribute. Current attribute count is '.$attributeCount.' but the maximum is '.$maxAttributes.'. Remove some attributes to free up space.';
            throw new LimitException($this->message);
        }

        if ($maxWidth > 0 && $attributeWidth >= $maxWidth) {
            $this->message = 'Row width limit reached. Cannot create new attribute. Current row width is '.$attributeWidth.' bytes but the maximum is '.$maxWidth.' bytes. Reduce the size of existing attributes or remove some attributes to free up space.';
            throw new LimitException($this->message);
        }

        return true;
    }

    /**
     * Check attribute type and type-specific constraints
     *
     * @throws DatabaseException
     */
    public function checkType(Attribute $attribute): bool
    {
        $type = $attribute->type;
        $size = $attribute->size ?? 0;
        $signed = $attribute->signed;
        $array = $attribute->array;
        $default = $attribute->default;

        switch ($type) {
            case ColumnType::Id:
                break;

            case ColumnType::String:
                if ($size > $this->profile->limits->string) {
                    $this->message = 'Max size allowed for string is: '.number_format($this->profile->limits->string);
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::Varchar:
                if ($size > $this->profile->limits->varchar) {
                    $this->message = 'Max size allowed for varchar is: '.number_format($this->profile->limits->varchar);
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
                $limit = $signed ? $this->profile->limits->integer / 2 : $this->profile->limits->integer;
                if ($size > $limit) {
                    $this->message = 'Max size allowed for int is: '.number_format($limit);
                    throw new DatabaseException($this->message);
                }
                break;

            case ColumnType::BigInteger:
            case ColumnType::Float:
            case ColumnType::Double:
            case ColumnType::Boolean:
            case ColumnType::Datetime:
            case ColumnType::Relationship:
                break;

            case ColumnType::Object:
                if (! $this->profile->supports(Capability::Objects)) {
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
                if (! $this->profile->hasFeature(Feature\Spatial::class)) {
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
                if (! $this->profile->supports(Capability::Vectors)) {
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
                $this->message = 'Unknown attribute type: '.$type->value.'. Must be one of '.\implode(', ', \array_map(
                    Attribute::storedType(...),
                    Attribute::availableTypes($this->profile),
                ));
                throw new DatabaseException($this->message);
        }

        return true;
    }

    /**
     * Check default value constraints and type matching
     *
     * @throws DatabaseException
     */
    public function checkDefaultValue(Attribute $attribute): bool
    {
        $default = $attribute->default;
        $type = $attribute->type;
        $signed = $attribute->signed;

        if (\is_null($default)) {
            return true;
        }

        if ($attribute->required) {
            $this->message = 'Cannot set a default value for a required attribute';
            throw new DatabaseException($this->message);
        }

        if ($this->isJsonDocumentDefault($attribute)) {
            $this->checkJsonEncodable($attribute);

            return true;
        }

        // Vectors, spatial types and objects store their values as arrays.
        if (\is_array($default) && ! $attribute->array && ! \in_array($type, [ColumnType::Vector, ColumnType::Object, ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon], true)) {
            $this->message = 'Cannot set an array default value for a non-array attribute';
            throw new DatabaseException($this->message);
        }

        $this->validateDefaultTypes($type, $default, $signed);

        return true;
    }

    /**
     * The json filter writes arrays, stdClass objects and documents to a string column as their
     * JSON encoding, so such a default is checked for encodability rather than against the column type.
     */
    private function isJsonDocumentDefault(Attribute $attribute): bool
    {
        $default = $attribute->default;

        return \in_array(self::JSON_FILTER, $attribute->filters, true)
            && ! $attribute->array
            && \in_array($attribute->type, self::STRING_TYPES, true)
            && (\is_array($default) || $default instanceof stdClass || $default instanceof Document);
    }

    /**
     * @throws DatabaseException
     */
    private function checkJsonEncodable(Attribute $attribute): void
    {
        $default = $attribute->default;

        if (\json_encode($default instanceof Document ? $default->getArrayCopy() : $default) === false) {
            $this->message = 'Default value of json attribute "'.$attribute->key.'" is not JSON-encodable: '.\json_last_error_msg();
            throw new DatabaseException($this->message);
        }
    }

    /**
     * Function to validate if the default value of an attribute matches its attribute type
     *
     * @param  ColumnType  $type  Type of the attribute
     * @param  mixed  $default  Default value of the attribute
     *
     * @throws DatabaseException
     */
    protected function validateDefaultTypes(ColumnType $type, mixed $default, bool $signed = true): void
    {
        $defaultType = \gettype($default);

        if ($defaultType === 'NULL') {
            // Disable null. No validation required
            return;
        }

        if ($defaultType === 'array') {
            if (\in_array($type, self::SPATIAL_TYPES, true)) {
                $spatial = new Spatial($type->value);
                if (! $spatial->isValid($default)) {
                    $this->message = 'Invalid default value: '.$spatial->getDescription();
                    throw new DatabaseException($this->message);
                }

                return;
            }

            if ($type !== ColumnType::Object) {
                /** @var array<mixed> $default */
                foreach ($default as $value) {
                    $this->validateDefaultTypes($type, $value, $signed);
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
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.Attribute::storedType($type);
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Integer:
            case ColumnType::Boolean:
                if ($type->value !== $defaultType) {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.Attribute::storedType($type);
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::BigInteger:
                if (! (new BigInt($signed, $this->profile->supports(Capability::UnsignedBigInt)))->isValid($default)) {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.Attribute::storedType($type);
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Float:
            case ColumnType::Double:
                if ($defaultType !== 'double') {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.Attribute::storedType($type);
                    throw new DatabaseException($this->message);
                }
                break;
            case ColumnType::Datetime:
                if ($defaultType !== 'string') {
                    $this->message = 'Default value '.json_encode($default).' does not match given type '.Attribute::storedType($type);
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
                    Attribute::storedType(ColumnType::BigInteger),
                    ColumnType::Float->value,
                    ColumnType::Double->value,
                    ColumnType::Boolean->value,
                    ColumnType::Datetime->value,
                    ColumnType::Relationship->value,
                ];
                if ($this->profile->supports(Capability::Vectors)) {
                    $supportedTypes[] = ColumnType::Vector->value;
                }
                if ($this->profile->hasFeature(Feature\Spatial::class)) {
                    \array_push($supportedTypes, ColumnType::Point->value, ColumnType::Linestring->value, ColumnType::Polygon->value);
                }
                $this->message = 'Unknown attribute type: '.$type->value.'. Must be one of '.implode(', ', $supportedTypes);
                throw new DatabaseException($this->message);
        }
    }
}
