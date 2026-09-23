<?php

namespace Utopia\Database\Validator\Query;

use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * Validates aggregate query methods ensuring the aggregated attribute exists in the schema.
 */
class Aggregate extends Base
{
    use JoinedAttributes;

    public const int MAX_ALIAS_LENGTH = 63;

    private const ALIAS_PATTERN = '/^[A-Za-z_][A-Za-z0-9_]*$/';

    private const array NUMERIC_METHODS = [
        Method::Sum,
        Method::Avg,
        Method::Stddev,
        Method::StddevPop,
        Method::StddevSamp,
        Method::Variance,
        Method::VarPop,
        Method::VarSamp,
    ];

    private const array BITWISE_METHODS = [
        Method::BitAnd,
        Method::BitOr,
        Method::BitXor,
    ];

    /**
     * @var array<string, true>
     */
    protected array $schema = [];

    /**
     * The type of each attribute that holds a single number.
     *
     * @var array<string, ColumnType>
     */
    protected array $numeric = [];

    /**
     * @param  array<Document>  $attributes
     */
    public function __construct(array $attributes = [], protected bool $supportForAttributes = true)
    {
        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));

            if (\is_string($key)) {
                $this->schema[$key] = true;
            }
        }

        foreach (Database::internalAttributes() as $attribute) {
            $this->schema[$attribute->key] = true;
        }

        $this->numeric = $this->numericTypes($attributes);
    }

    public function getMethodType(): string
    {
        return self::METHOD_TYPE_AGGREGATE;
    }

    protected function isValidQuery(Query $query): bool
    {
        $attribute = $query->getAttribute();

        if ($attribute === '*' && $query->getMethod() !== Method::Count) {
            $this->message = 'Only count can aggregate "*"';

            return false;
        }

        if (
            $attribute !== '*'
            && $this->supportForAttributes
            && ! isset($this->schema[$attribute])
            && ! $this->isJoinedAttribute($attribute)
        ) {
            return false;
        }

        if (! $this->isValidOperand($query)) {
            return false;
        }

        $alias = $query->getValues()[0] ?? null;

        if ($alias !== null && (! \is_string($alias) || \preg_match(self::ALIAS_PATTERN, $alias) !== 1)) {
            $this->message = 'Invalid aggregate alias';

            return false;
        }

        if (\is_string($alias) && \strlen($alias) > self::MAX_ALIAS_LENGTH) {
            $this->message = 'Aggregate alias is too long: at most '.self::MAX_ALIAS_LENGTH.' characters are allowed';

            return false;
        }

        return true;
    }

    /**
     * Arithmetic aggregates need a number and the bitwise ones an integer. An attribute outside
     * this collection's schema, a joined or schemaless one, has no type to check here.
     */
    private function isValidOperand(Query $query): bool
    {
        $method = $query->getMethod();
        $attribute = $query->getAttribute();
        $bitwise = \in_array($method, self::BITWISE_METHODS, true);

        if ((! $bitwise && ! \in_array($method, self::NUMERIC_METHODS, true)) || ! isset($this->schema[$attribute])) {
            return true;
        }

        $type = $this->numeric[$attribute] ?? null;

        if ($type === null) {
            $this->message = 'Aggregate '.$method->value.' requires a numeric attribute that is not an array: '.$attribute;

            return false;
        }

        if ($bitwise && ! Attribute::isIntegerType($type)) {
            $this->message = 'Aggregate '.$method->value.' requires an integer attribute that is not an array: '.$attribute;

            return false;
        }

        return true;
    }

    /**
     * @param  array<Document>  $attributes
     * @return array<string, ColumnType>
     */
    private function numericTypes(array $attributes): array
    {
        $types = [];

        foreach ($attributes as $attribute) {
            $key = $attribute->getAttribute('key', $attribute->getAttribute(Document::ID));
            $type = $attribute->getAttribute('type');

            if (! \is_string($key) || ! ($type instanceof ColumnType || \is_string($type)) || (bool) $attribute->getAttribute('array', false)) {
                continue;
            }

            $type = Attribute::tryNormalizeType($type);

            if ($type !== null && Attribute::isNumericType($type)) {
                $types[$key] = $type;
            }
        }

        return $types;
    }
}
