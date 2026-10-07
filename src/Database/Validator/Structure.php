<?php

namespace Utopia\Database\Validator;

use Closure;
use Exception;
use Utopia\Database\Adapter\Profile;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
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

/**
 * Validates document structure against collection schema including required attributes, types, and formats.
 */
class Structure extends Validator
{
    /**
     * @var list<Attribute>|null
     */
    private static ?array $internalAttributes = null;

    /**
     * @var array<string, array{callback: callable, type: string}>
     */
    protected static array $formats = [];

    protected string $message = 'General Error';

    /**
     * Internal and collection attributes by key, built on first use. A validator is built for one schema:
     * construct a new one after the collection's attributes change.
     *
     * @var array<string, Attribute>|null
     */
    private ?array $definitions = null;

    /**
     * @var array<string, array<string, mixed>>
     */
    private array $formatDefinitions = [];

    /**
     * @var array<string, true>
     */
    private readonly array $storedAttributes;

    private readonly bool $supportForAttributes;

    private readonly bool $supportUnsignedBigInt;

    /**
     * Structure constructor.
     *
     * @param  list<string>  $storedAttributes  Attributes whose values are the stored ones, unchanged by the
     *                                         write: they are not validated again, as the rules may have
     *                                         tightened since those values were stored.
     */
    public function __construct(
        protected readonly Document $collection,
        private readonly Profile $profile,
        private readonly ?Document $currentDocument = null,
        array $storedAttributes = [],
    ) {
        $this->storedAttributes = \array_fill_keys($storedAttributes, true);
        $this->supportForAttributes = $profile->supports(Capability::DefinedAttributes);
        $this->supportUnsignedBigInt = $profile->supports(Capability::UnsignedBigInt);
    }

    /**
     * Add a new Validator
     * Stores a callback and required params to create Validator
     *
     * @param  Closure(array<string, mixed>): Validator  $callback
     * @param  ColumnType  $type  Primitive data type for validation
     */
    public static function addFormat(string $name, Closure $callback, ColumnType $type): void
    {
        self::$formats[$name] = [
            'callback' => $callback,
            'type' => $type->value,
        ];
    }

    /**
     * Check if validator has been added
     */
    public static function hasFormat(string $name, ColumnType $type): bool
    {
        if (isset(self::$formats[$name]) && self::$formats[$name]['type'] === $type->value) {
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
    public static function getFormat(string $name, ColumnType $type): array
    {
        if (isset(self::$formats[$name])) {
            if (self::$formats[$name]['type'] !== $type->value) {
                throw new DatabaseException('Format "'.$name.'" not available for attribute type "'.$type->value.'"');
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
            $this->message = 'Missing collection attribute '.Document::COLLECTION;

            return false;
        }

        if (empty($this->collection->getId()) || $this->collection->getCollection() !== Database::METADATA) {
            $this->message = 'Collection not found';

            return false;
        }

        $structure = $document->getArrayCopy();
        $definitions = $this->definitions();

        if (! $this->checkForAllRequiredValues($structure, $definitions)) {
            return false;
        }

        if (! $this->checkForUnknownAttributes($structure, $definitions)) {
            return false;
        }

        if (! $this->checkForInvalidAttributeValues($document, $structure, $definitions)) {
            return false;
        }

        return true;
    }

    /**
     * @return list<Attribute>
     */
    protected static function internalAttributes(): array
    {
        return self::$internalAttributes ??= [
            Attribute::string(Document::ID, 255),
            Attribute::id(Document::SEQUENCE),
            Attribute::string(Document::COLLECTION, 255, required: true),
            Attribute::id(Document::TENANT),
            Attribute::string(Document::PERMISSIONS, 67000, array: true),
            Attribute::datetime(Document::CREATED_AT, required: true),
            Attribute::datetime(Document::UPDATED_AT, required: true),
        ];
    }

    /**
     * @return array<string, Attribute>
     */
    protected function definitions(): array
    {
        if ($this->definitions !== null) {
            return $this->definitions;
        }

        $collection = Collection::fromDocument($this->collection);

        $definitions = [];
        foreach (self::internalAttributes() as $attribute) {
            $definitions[$attribute->key] = $attribute;
        }
        foreach ($collection->attributes() as $attribute) {
            $definitions[$attribute->key] = $attribute;
        }

        return $this->definitions = $definitions;
    }

    /**
     * @param  array<string, mixed>  $structure
     * @param  array<Attribute>  $attributes
     */
    protected function checkForAllRequiredValues(array $structure, array $attributes): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }

        foreach ($attributes as $attribute) {
            if ($attribute->required && ! isset($structure[$attribute->key])) {
                $this->message = 'Missing required attribute "'.$attribute->key.'"';

                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<string, mixed>  $structure
     * @param  array<string, Attribute>  $definitions
     */
    protected function checkForUnknownAttributes(array $structure, array $definitions): bool
    {
        if (! $this->supportForAttributes) {
            return true;
        }
        foreach ($structure as $key => $value) {
            if (! isset($definitions[$key])) {
                $this->message = 'Unknown attribute: "'.$key.'"';

                return false;
            }
        }

        return true;
    }

    /**
     * @param  array<string, mixed>  $structure
     * @param  array<string, Attribute>  $definitions
     */
    protected function checkForInvalidAttributeValues(Document $document, array $structure, array $definitions): bool
    {
        foreach ($structure as $key => $value) {
            if (Operator::isOperator($value)) {
                /** @var Operator $value */
                $value->setAttribute($key);

                $operatorValidator = new OperatorValidator(
                    $this->collection,
                    $this->currentDocument,
                    $this->supportUnsignedBigInt,
                );
                if (! $operatorValidator->isValid($value)) {
                    $this->message = $operatorValidator->getDescription();

                    return false;
                }

                continue;
            }

            if (isset($this->storedAttributes[$key])) {
                continue;
            }

            $attribute = $definitions[$key] ?? null;
            if ($attribute === null) {
                continue;
            }

            $type = $attribute->type;
            $size = $attribute->size ?? 0;
            $signed = $attribute->signed;
            $required = $attribute->required;

            if ($required === false && is_null($value)) {
                continue;
            }

            if ($type === ColumnType::Relationship) {
                continue;
            }

            if ($type === ColumnType::BigInteger && \is_string($value) && BigInt::fitsPhpInt($value, $signed)) {
                $value = (int) $value;
                $document->setAttribute($key, $value);
            }

            $validators = [];

            switch ($type) {
                case ColumnType::Id:
                    $validators[] = new Sequence($this->profile->limits->idType->value, $key === Document::SEQUENCE);
                    break;

                case ColumnType::Text:
                    $validators[] = new ByteLength($size);
                    $validators[] = new ByteLength(Database::MAX_TEXT_BYTES);
                    break;

                case ColumnType::MediumText:
                    $validators[] = new ByteLength($size);
                    $validators[] = new ByteLength(Database::MAX_MEDIUMTEXT_BYTES);
                    break;

                case ColumnType::LongText:
                    $validators[] = new ByteLength($size);
                    $validators[] = new ByteLength(Database::MAX_LONGTEXT_BYTES);
                    break;

                case ColumnType::Varchar:
                case ColumnType::String:
                    $validators[] = new Text($size, min: 0);
                    break;

                case ColumnType::Integer:
                    $bits = $size >= 8 ? 64 : 32;
                    // For 64-bit unsigned, use signed since PHP doesn't support true 64-bit unsigned
                    // The Range validator will restrict to positive values only
                    $unsigned = ! $signed && $bits < 64;
                    $validators[] = new Integer(false, $bits, $unsigned);
                    $max = $bits === 64 ? Database::MAX_BIG_INT : Database::MAX_INT;
                    $min = $signed ? -$max : 0;
                    $validators[] = new Range($min, $max, ColumnType::Integer->value);
                    break;

                case ColumnType::BigInteger:
                    $validators[] = new BigInt($signed, $this->supportUnsignedBigInt);
                    break;

                case ColumnType::Float:
                case ColumnType::Double:
                    // We need both Float and Range because Range implicitly casts non-numeric values
                    $validators[] = new FloatValidator();
                    $min = $signed ? -Database::MAX_DOUBLE : 0;
                    $validators[] = new Range($min, Database::MAX_DOUBLE, ColumnType::Double->value);
                    break;

                case ColumnType::Boolean:
                    $validators[] = new Boolean();
                    break;

                case ColumnType::Datetime:
                    $validators[] = new DatetimeValidator(
                        min: $this->profile->limits->minDateTime,
                        max: $this->profile->limits->maxDateTime
                    );
                    break;

                case ColumnType::Object:
                    $validators[] = new ObjectValue();
                    break;

                case ColumnType::Point:
                case ColumnType::Linestring:
                case ColumnType::Polygon:
                    $validators[] = new Spatial($type->value);
                    break;

                case ColumnType::Vector:
                    $validators[] = new Vector($size);
                    break;

                default:
                    if ($this->supportForAttributes) {
                        $this->message = 'Unknown attribute type "'.$type->value.'"';

                        return false;
                    }
            }

            $format = $attribute->format?->name;
            $label = $format !== null ? 'format' : 'type';

            if ($format !== null) {
                $definition = self::getFormat($format, $type);
                /** @var Validator $formatValidator */
                $formatValidator = $definition['callback']($this->formatDefinitions[$key] ??= $attribute->toDocument()->getArrayCopy());
                $validators[] = $formatValidator;
            }

            if ($attribute->array) {
                if (! $required && ((is_array($value) && empty($value)) || is_null($value))) {
                    continue;
                }

                if (! \is_array($value) || ! \array_is_list($value)) {
                    $this->message = 'Attribute "'.$key.'" must be an array';

                    return false;
                }

                foreach ($value as $x => $child) {
                    if (! $required && is_null($child)) {
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
