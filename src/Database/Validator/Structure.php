<?php

namespace Utopia\Database\Validator;

use Closure;
use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Datetime as DatetimeValidator;
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
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$sequence',
            'type' => Database::VAR_ID,
            'size' => 0,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$collection',
            'type' => Database::VAR_STRING,
            'size' => 255,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$tenant',
            'type' => Database::VAR_INTEGER, // ? VAR_ID
            'size' => 8,
            'required' => false,
            'default' => null,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$permissions',
            'type' => Database::VAR_STRING,
            'size' => 67000, // medium text
            'required' => false,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ],
        [
            '$id' => '$createdAt',
            'type' => Database::VAR_DATETIME,
            'size' => 0,
            'required' => true,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$updatedAt',
            'type' => Database::VAR_DATETIME,
            'size' => 0,
            'required' => true,
            'signed' => false,
            'array' => false,
            'filters' => [],
        ]
    ];

    /**
     * @var array<string, array{callback: callable, type: string}>
     */
    protected static array $formats = [];

    /**
     * @var string
     */
    protected string $message = 'General Error';

    /**
     * Structure constructor.
     *
     */
    public function __construct(
        protected readonly Document $collection,
        private readonly string $idAttributeType,
        private readonly \DateTime $minAllowedDate = new \DateTime('0000-01-01'),
        private readonly \DateTime $maxAllowedDate = new \DateTime('9999-12-31'),
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
     * @param string $name
     * @param Closure $callback Callback that accepts $params in order and returns \Utopia\Validator
     * @param string $type Primitive data type for validation
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
     *
     * @param string $name
     *
     * @return bool
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
     * @param string $name
     * @param string $type
     *
     * @return array{callback: callable, type: string}
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
     *
     * @param string $name
     */
    public static function removeFormat(string $name): void
    {
        unset(self::$formats[$name]);
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
        return 'Invalid document structure: '.$this->message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param mixed $document
     *
     * @return bool
     */
    public function isValid($document): bool
    {
        if (!$document instanceof Document) {
            $this->message = 'Value must be an instance of Document';
            return false;
        }

        if (empty($document->getCollection())) {
            $this->message = 'Missing collection attribute $collection';
            return false;
        }

        if (empty($this->collection->getId()) || Database::METADATA !== $this->collection->getCollection()) {
            $this->message = 'Collection not found';
            return false;
        }

        $keys = [];
        $structure = $document->getArrayCopy();
        $attributes = \array_merge($this->attributes, $this->collection->getAttribute('attributes', []));

        if (!$this->checkForAllRequiredValues($structure, $attributes, $keys)) {
            return false;
        }

        if (!$this->checkForUnknownAttributes($structure, $keys)) {
            return false;
        }

        if (!$this->checkForInvalidAttributeValues($structure, $keys)) {
            return false;
        }

        return true;
    }

    /**
     * Check for all required values
     *
     * @param array<string, mixed> $structure
     * @param array<string, mixed> $attributes
     * @param array<string, mixed> $keys
     *
     * @return bool
     */
    protected function checkForAllRequiredValues(array $structure, array $attributes, array &$keys): bool
    {
        foreach ($attributes as $key => $attribute) { // Check all required attributes are set
            $name = $attribute['$id'] ?? '';
            $required = $attribute['required'] ?? false;

            $keys[$name] = $attribute; // List of allowed attributes to help find unknown ones

            if ($required && !isset($structure[$name])) {
                $this->message = 'Missing required attribute "'.$name.'"';
                return false;
            }
        }

        return true;
    }

    /**
     * Check for Unknown Attributes
     *
     * @param array<string, mixed> $structure
     * @param array<string, mixed> $keys
     *
     * @return bool
     */
    protected function checkForUnknownAttributes(array $structure, array $keys): bool
    {
        foreach ($structure as $key => $value) {
            if (!array_key_exists($key, $keys)) { // Check no unknown attributes are set
                $this->message = 'Unknown attribute: "'.$key.'"';
                return false;
            }
        }

        return true;
    }

    /**
     * Check for invalid attribute values
     *
     * @param array<string, mixed> $structure
     * @param array<string, mixed> $keys
     *
     * @return bool
     */
    protected function checkForInvalidAttributeValues(array $structure, array $keys): bool
    {
        foreach ($structure as $key => $value) {
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

            if ($type === Database::VAR_RELATIONSHIP) {
                continue;
            }

            $validators = [];

            switch ($type) {
                case Database::VAR_ID:
                    $validators[] = new Sequence($this->idAttributeType, $attribute['$id'] === '$sequence');
                    break;

                case Database::VAR_STRING:
                    $validators[] = new Text($size, min: 0);
                    break;

                case Database::VAR_INTEGER:
                    // We need both Integer and Range because Range implicitly casts non-numeric values
                    $validators[] = new Integer();
                    $max = $size >= 8 ? Database::BIG_INT_MAX : Database::INT_MAX;
                    $min = $signed ? -$max : 0;
                    $validators[] = new Range($min, $max, Database::VAR_INTEGER);
                    break;

                case Database::VAR_FLOAT:
                    // We need both Float and Range because Range implicitly casts non-numeric values
                    $validators[] = new FloatValidator();
                    $min = $signed ? -Database::DOUBLE_MAX : 0;
                    $validators[] =  new Range($min, Database::DOUBLE_MAX, Database::VAR_FLOAT);
                    break;

                case Database::VAR_BOOLEAN:
                    $validators[] = new Boolean();
                    break;

                case Database::VAR_DATETIME:
                    $validators[] = new DatetimeValidator(
                        min: $this->minAllowedDate,
                        max: $this->maxAllowedDate
                    );
                    break;

                case Database::VAR_GEOMETRY:
                case Database::VAR_POINT:
                case Database::VAR_LINESTRING:
                case Database::VAR_POLYGON:
                    $validators[] = new \Utopia\Database\Validator\Spatial($type);
                    break;

                default:
                    $this->message = 'Unknown attribute type "'.$type.'"';
                    return false;
            }

            /** Error message label, either 'format' or 'type' */
            $label = ($format) ? 'format' : 'type';

            if ($format) {
                // Format encoded as json string containing format name and relevant format options
                $format = self::getFormat($format, $type);
                $validators[] = $format['callback']($attribute);
            }

            if ($array) { // Validate attribute type for arrays - format for arrays handled separately
                if (!$required && ((is_array($value) && empty($value)) || is_null($value))) { // Allow both null and [] for optional arrays
                    continue;
                }

                if (!\is_array($value) || !\array_is_list($value)) {
                    $this->message = 'Attribute "'.$key.'" must be an array';
                    return false;
                }

                foreach ($value as $x => $child) {
                    if (!$required && is_null($child)) { // Allow null value to optional params
                        continue;
                    }

                    foreach ($validators as $validator) {
                        if (!$validator->isValid($child)) {
                            $this->message = 'Attribute "'.$key.'[\''.$x.'\']" has invalid '.$label.'. '.$validator->getDescription();
                            return false;
                        }
                    }
                }
            } else {
                foreach ($validators as $validator) {
                    if (!$validator->isValid($value)) {
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
     *
     * @return bool
     */
    public function isArray(): bool
    {
        return false;
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
        return self::TYPE_ARRAY;
    }
}
