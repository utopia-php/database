<?php

namespace Utopia\Database\Validator;

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
     * @var Validator[]
     */
    static protected $validators = [];

    /**
     * @var string
     */
    protected $message = 'General Error';

    /**
     * Structure constructor.
     *
     * @param Document $collection
     */
    public function __construct(Document $collection)
    {
        $this->collection = $collection;
    }

    /**
     * Remove a Validator
     * 
     * @param string $name
     */
    static public function getValidators()
    {
        return self::$validators;
    }

    /**
     * Add a new Validator
     * 
     * @param string $name
     * @param Validator $validator
     * @param string $type
     */
    static public function addValidator(string $name, Validator $validator, string $type)
    {
        self::$validators[$name] = [
            'validator' => $validator,
            'type' => $type,
        ];
    }

    /**
     * Remove a Validator
     * 
     * @param string $name
     */
    static public function removeValidator(string $name)
    {
        unset(self::$validators[$name]);
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @return string
     */
    public function getDescription()
    {
        return 'Invalid document structure: '.$this->message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param Document $document
     *
     * @return bool
     */
    public function isValid($document)
    {
        if(!$document instanceof Document) {
            $this->message = 'Value must be an instance of Document';
            return false;
        }
        
        if (empty($document->getCollection())) {
            $this->message = 'Missing collection attribute $collection';
            return false;
        }
        
        if (empty($this->collection->getId()) || Database::COLLECTIONS !== $this->collection->getCollection()) {
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
                $this->message = 'Unknown attribute: "'. '"'.$key.'"';
                return false;
            }

            $attribute = $keys[$key] ?? [];
            $type = $attribute['type'] ?? '';
            $array = $attribute['array'] ?? false;

            switch ($type) {
                case Database::VAR_STRING:
                    $validator = new Text(0);
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

            if($array) { // Validate attribute type
                if(!is_array($value)) {
                    $this->message = 'Attribute "'.$key.'" must be an array';
                    return false;
                }

                foreach ($value as $x => $child) {
                    if(!$validator->isValid($child)) {
                        $this->message = 'Attribute "'.$key.'"["'.$x.'"] has invalid type. '.$validator->getDescription();
                        return false;
                    }
                }
            }
            else {
                if(!$validator->isValid($value)) {
                    $this->message = 'Attribute "'.$key.'" has invalid type. '.$validator->getDescription();
                    return false;
                }
            }

            // TODO check for length / size
            // TODO check for specific validation
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
