<?php

namespace Utopia\Database\Hook;

use InvalidArgumentException;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Hook\Filter;

final readonly class PermissionAllowNullUid implements Filter
{
    private const IDENTIFIER_PATTERN = '/^[a-zA-Z_][a-zA-Z0-9_.\-]*$/';

    public function __construct(
        private Filter $filter,
        private string $documentColumn,
        private string $quoteChar = '`',
    ) {
        if (! \preg_match(self::IDENTIFIER_PATTERN, $documentColumn)) {
            throw new InvalidArgumentException('Invalid column name: '.$documentColumn);
        }
    }

    public function filter(string $table): Condition
    {
        $original = $this->filter->filter($table);
        $quoted = $this->quoteIdentifier($this->documentColumn);

        return new Condition(
            '('.$original->expression.' OR '.$quoted.' IS NULL)',
            $original->bindings,
        );
    }

    private function quoteIdentifier(string $identifier): string
    {
        $q = $this->quoteChar;
        $parts = \explode('.', $identifier);
        $quoted = \array_map(
            fn (string $part): string => $q.\str_replace($q, $q.$q, $part).$q,
            $parts,
        );

        return \implode('.', $quoted);
    }
}
