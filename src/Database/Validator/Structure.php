<?php

namespace Utopia\Database\Validator;

use Closure;
use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
use Utopia\Database\Validator\Operator as OperatorValidator;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Range;
use Utopia\Validator\Text;

class Structure extends Validator
{
    /**
     * @var array<array<string, mixed>>
     */
    protected array $attributes = [
        [
            '$id' => '$id',
            'type' => 'string',
            'size' => 255,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$sequence',
            'type' => 'id',
            'size' => 0,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$collection',
            'type' => 'string',
            'size' => 255,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$tenant',
            'type' => 'integer',
            'size' => 8,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$permissions',
            'type' => 'string',
            'size' => 67000,
            'required' => false,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ],
        [
            '$id' => '$createdAt',
            'type' => 'datetime',
            'size' => 0,
            'required' => true,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$updatedAt',
            'type' => 'datetime',
            'size' => 0,
            'required' => true,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ],
    ];

    /**
     * @var array<string, array{callback: callable, type: string}>
     */
    protected static array $formats = [];

    protected string $message = 'General Error';

    /**
     * Structure constructor.
     */
    public function __construct(
        protected readonly Document $collection,
        private readonly string $idAttributeType,
        private readonly \DateTime $minAllowedDate = new \DateTime('0000-01-01'),
        private readonly \DateTime $maxAllowedDate = new \DateTime('9999-12-31'),
        private bool $supportForAttributes = true,
        private readonly ?Document $currentDocument = null
    ) {
    }

    /**
     * Remove a Validator
     *
     * @return array<string, array{callback: callable, type: string}>
     */
    public static function getFormats(): array
    {
        return self::$formats;
    }

    /**
     * Add a new Validator
     * Stores a callback and required params to create Validator
     *
     * @param  Closure  $callback  Callback that accepts $params in order and returns \Utopia\Validator
     * @param  string  $type  Primitive data type for validation
     */
    public static function addFormat(string $name, Closure $callback, string $type): void
    {
        self::$formats[$name] = [
            'callback' => $callback,
            'type' => $type,
        ];
    }

    /**
     * Check if validator has been added
     */
    public static function hasFormat(string $name, string $type): bool
    {
        if (isset(self::$formats[$name]) && self::$formats[$name]['type'] === $type) {
            return true;
        }

        return false;
    }

    /**
     * Get a Format array to create Validator
     *
     *
     * @return array{callback: callable, type: string}
     *
     * @throws Exception
     */
    public static function getFormat(string $name, string $type): array
    {
        if (isset(self::$formats[$name])) {
            if (self::$formats[$name]['type'] !== $type) {
                throw new DatabaseException('Format "'.$name.'" not available for attribute type "'.$type.'"');
            }

            return self::$formats[$name];
        }

        throw new DatabaseException('Unknown format validator "'.$name.'"');
    }

    /**
     * Remove a Validator
     */
    public static function removeFormat(string $name): void
    {
        unset(self::$formats[$name]);
    }

    /**
     * Get Description.
     *
     * Returns validator description
     */
    public function getDescription(): string
    {
        return 'Invalid document structure: '.$this->message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param  mixed  $document
     */
    public function isValid($document): bool
    {
        if (! $document instanceof Document) {
            $this->message = 'Value must be an instance of Document';

            return false;
        }

        if (empty($document->getCollection())) {
            $this->message = 'Missing collection attribute $collection';

            return false;
        }

        if (empty($this->collection->getId()) || $this->collection->getCollection() !== Database::METADATA) {
            $this->message = 'Collection not found';

            return false;
        }

        $keys = [];
        $structure = $document->getArrayCopy();
        $attributes = \array_merge($this->attributes, $this->collection->getAttribute('attributes', []));

        if (! $this->checkForAllRequiredValues($structure, $attributes, $keys)) {
            return false;
        }

        if (! $this->checkForUnknownAttributes($structure, $keys)) {
            return false;
        }

        if (! $this->checkForInvalidAttributeValues($structure, $keys)) {
            return false;
        }

        return true;
    }

    /**
     * Check for all required values
     *
     * @param  array<string, mixed>  $structure
     * @param  array<string, mixed>  $attributes
     * @param  array<string, mixed>  $keys
     */
    protected function checkForAllRequiredValues(array $structure, array $attributes, array &$keys): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }

        foreach ($attributes as $attribute) { // Check all required attributes are set
            $name = $attribute['$id'] ?? '';
            $required = $attribute['required'] ?? false;

            $keys[$name] = $attribute; // List of allowed attributes to help find unknown ones

            if ($required && ! isset($structure[$name])) {
                $this->message = 'Missing required attribute "'.$name.'"';

                return false;
            }
        }

        return true;
    }

    /**
     * Check for Unknown Attributes
     *
     * @param  array<string, mixed>  $structure
     * @param  array<string, mixed>  $keys
     */
    protected function checkForUnknownAttributes(array $structure, array $keys): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        foreach ($structure as $key => $value) {
            if (! array_key_exists($key, $keys)) { // Check no unknown attributes are set
                $this->message = 'Unknown attribute: "'.$key.'"';

                return false;
            }
        }

        return true;
    }

    /**
     * Check for invalid attribute values
     *
     * @param  array<string, mixed>  $structure
     * @param  array<string, mixed>  $keys
     */
    protected function checkForInvalidAttributeValues(array $structure, array $keys): bool
    {
        foreach ($structure as $key => $value) {
            if (Operator::isOperator($value)) {
                // Set the attribute name on the operator for validation
                $value->setAttribute($key);

                $operatorValidator = new OperatorValidator($this->collection, $this->currentDocument);
                if (! $operatorValidator->isValid($value)) {
                    $this->message = $operatorValidator->getDescription();

                    return false;
                }

                continue;
            }

            $attribute = $keys[$key] ?? [];
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            $format = $attribute['format'] ?? '';
            $required = $attribute['required'] ?? false;
            $size = $attribute['size'] ?? 0;
            $signed = $attribute['signed'] ?? true;

            if ($required === false && is_null($value)) { // Allow null value to optional params
                continue;
            }

            if ($type === ColumnType::Relationship->value) {
                continue;
            }

            $validators = [];

            switch ($type) {
                case ColumnType::Id->value:
                    $validators[] = new Sequence($this->idAttributeType, $attribute['$id'] === '$sequence');
                    break;

                case ColumnType::Varchar->value:
                case ColumnType::Text->value:
                case ColumnType::MediumText->value:
                case ColumnType::LongText->value:
                case ColumnType::String->value:
                    $validators[] = new Text($size, min: 0);
                    break;

                case ColumnType::Integer->value:
                    // Determine bit size based on attribute size in bytes
                    $bits = $size >= 8 ? 64 : 32;
                    // For 64-bit unsigned, use signed since PHP doesn't support true 64-bit unsigned
                    // The Range validator will restrict to positive values only
                    $unsigned = ! $signed && $bits < 64;
                    $validators[] = new Integer(false, $bits, $unsigned);
                    $max = $size >= 8 ? Database::MAX_BIG_INT : Database::MAX_INT;
                    $min = $signed ? -$max : 0;
                    $validators[] = new Range($min, $max, ColumnType::Integer->value);
                    break;

                case ColumnType::Double->value:
                    // We need both Float and Range because Range implicitly casts non-numeric values
                    $validators[] = new FloatValidator();
                    $min = $signed ? -Database::MAX_DOUBLE : 0;
                    $validators[] = new Range($min, Database::MAX_DOUBLE, ColumnType::Double->value);
                    break;

                case ColumnType::Boolean->value:
                    $validators[] = new Boolean();
                    break;

                case ColumnType::Datetime->value:
                    $validators[] = new DatetimeValidator(
                        min: $this->minAllowedDate,
                        max: $this->maxAllowedDate
                    );
                    break;

                case ColumnType::Object->value:
                    $validators[] = new ObjectValidator();
                    break;

                case ColumnType::Point->value:
                case ColumnType::Linestring->value:
                case ColumnType::Polygon->value:
                    $validators[] = new Spatial($type);
                    break;

                case ColumnType::Vector->value:
                    $validators[] = new Vector($attribute['size'] ?? 0);
                    break;

                default:
                    if ($this->supportForAttributes) {
                        $this->message = 'Unknown attribute type "'.$type.'"';

                        return false;
                    }
            }

            /** Error message label, either 'format' or 'type' */
            $label = ($format) ? 'format' : 'type';

            if ($format) {
                // Format encoded as json string containing format name and relevant format options
                $format = self::getFormat($format, $type);
                $validators[] = $format['callback']($attribute);
            }

            if ($array) { // Validate attribute type for arrays - format for arrays handled separately
                if (! $required && ((is_array($value) && empty($value)) || is_null($value))) { // Allow both null and [] for optional arrays
                    continue;
                }

                if (! \is_array($value) || ! \array_is_list($value)) {
                    $this->message = 'Attribute "'.$key.'" must be an array';

                    return false;
                }

                foreach ($value as $x => $child) {
                    if (! $required && is_null($child)) { // Allow null value to optional params
                        continue;
                    }

                    foreach ($validators as $validator) {
                        if (! $validator->isValid($child)) {
                            $this->message = 'Attribute "'.$key.'[\''.$x.'\']" has invalid '.$label.'. '.$validator->getDescription();

                            return false;
                        }
                    }
                }
            } else {
                foreach ($validators as $validator) {
                    if (! $validator->isValid($value)) {
                        $this->message = 'Attribute "'.$key.'" has invalid '.$label.'. '.$validator->getDescription();

                        return false;
                    }
                }
            }
        }

        return true;
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
     * Get Type
     *
     * Returns validator type.
     */
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}
