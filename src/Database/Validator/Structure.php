<?php

namespace Utopia\Database\Validator;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Validator;
use Utopia\Validator\Boolean;
use Utopia\Validator\FloatValidator;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

class Structure extends Validator
{
    /**
     * @var Document
     */
    protected $collection;

    /**
     * @var array
     */
    protected $attributes = [
        [
            '$id' => '$id',
            'type' => Database::VAR_STRING,
            'size' => 64,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$internalId',
            'type' => Database::VAR_STRING,
            'size' => 64,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$collection',
            'type' => Database::VAR_STRING,
            'size' => 64,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ],
        [
            '$id' => '$read',
            'type' => Database::VAR_STRING,
            'size' => 64,
            'required' => false,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ],
        [
            '$id' => '$write',
            'type' => Database::VAR_STRING,
            'size' => 64,
            'required' => false,
            'signed' => true,
            'array' => true,
            'filters' => [],
        ],
    ];

    /**
     * @var array
     */
    static protected $formats = [];

    /**
     * @var string
     */
    protected $message = 'General Error';

    /**
     * Structure constructor.
     *
     */
    public function __construct(Document $collection)
    {
        $this->collection = $collection;
    }

    /**
     * Remove a Validator
     * 
     * @param string $name
     * 
     * @return array
     */
    static public function getFormats(): array
    {
        return self::$formats;
    }

    /**
     * Add a new Validator
     * Stores a callback and required params to create Validator
     * 
     * @param string $name
     * @param \Closure $callback Callback that accepts $params in order and returns \Utopia\Validator
     * @param string $type Primitive data type for validation
     */
    static public function addFormat(string $name, \Closure $callback, string $type): void
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
    static public function hasFormat(string $name, string $type): bool
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
     * @return array
     */
    static public function getFormat(string $name, string $type): array
    {
        if(isset(self::$formats[$name])) {
            if(self::$formats[$name]['type'] !== $type) {
                throw new Exception('Format ("'.$name.'") not available for this attribute type ("'.$type.'")');
            }

            return self::$formats[$name];
        }

        throw new Exception('Unknown format validator: "'.$name.'"');
    }

    /**
     * Remove a Validator
     * 
     * @param string $name
     */
    static public function removeFormat(string $name): void
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
        if(!$document instanceof Document) {
            $this->message = 'Value must be an instance of Document';
            return false;
        }

        if (empty($document->getCollection())) {
            $this->message = 'Missing collection attribute $collection';
            return false;
        }

        if (empty($this->collection->getId()) || Database::METADATA !== $this->collection->getCollection()) {
            $this->message = 'Collection "'.$this->collection->getCollection().'" not found';
            return false;
        }

        $keys = [];
        $structure = $document->getArrayCopy();
        $attributes = \array_merge($this->attributes, $this->collection->getAttribute('attributes', []));

        foreach ($attributes as $key => $attribute) { // Check all required attributes are set
            $name = $attribute['$id'] ?? '';
            $required = $attribute['required'] ?? false;

            $keys[$name] = $attribute; // List of allowed attributes to help find unknown ones

            if($required && !isset($structure[$name])) {
                $this->message = 'Missing required attribute "'.$name.'"';
                return false;
            }
        }

        foreach ($structure as $key => $value) {
            if(!array_key_exists($key, $keys)) { // Check no unknown attributes are set
                $this->message = 'Unknown attribute: "'.$key.'"';
                return false;
            }

            $attribute = $keys[$key] ?? [];
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;
            $format = $attribute['format'] ?? '';
            $required = $attribute['required'] ?? false;

            if ($required === false && is_null($value)) { // Allow null value to optional params
                continue;
            }

            switch ($type) {
                case Database::VAR_STRING:
                    $size = $attribute['size'] ?? 0;
                    $validator = new Text($size);
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

                default:
                    $this->message = 'Unknown attribute type "'.$type.'"';
                    return false;
                    break;
            }

            /** @var string $label Error messasage label, either 'format' or 'type' */
            $label = ($format) ? 'format' : 'type';

            if ($format) {
                // Format encoded as json string containing format name and relevant format options
                $format = self::getFormat($format, $type);
                $validator = $format['callback']($attribute);
            }

            if($array) { // Validate attribute type for arrays - format for arrays handled separately
                if($required == false && empty($value)) { // Allow both null and [] for optional arrays
                    continue;
                }
                if(!is_array($value)) {
                    $this->message = 'Attribute "'.$key.'" must be an array';
                    return false;
                }

                foreach ($value as $x => $child) {
                    if($required == false && is_null($child)) { // Allow null value to optional params
                        continue;
                    }

                    if(!$validator->isValid($child)) {
                        $this->message = 'Attribute "'.$key.'[\''.$x.'\']" has invalid '.$label.'. '.$validator->getDescription();
                        return false;
                    }
                }
            }
            else {
                if(!$validator->isValid($value)) {
                    $this->message = 'Attribute "'.$key.'" has invalid '.$label.'. '.$validator->getDescription();
                    return false;
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
