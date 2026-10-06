<?php

namespace Utopia\Database\Builder;

/**
 * Prepares a fulltext search term as 7.x did: every character other than a letter, a number, an underscore or
 * whitespace separates words, so `foo/bar` and `foo,bar` search the words `foo` and `bar`. A term wrapped in double
 * quotes stays an exact search.
 */
trait PreparesSearchTerms
{
    private const string SEARCH_QUOTE = '"';

    private const string SEARCH_SEPARATORS = '/[^\p{L}\p{N}_\s]/u';

    private const string SEARCH_WHITESPACE = '/\s+/';

    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileSearchExpr(string $attribute, array $values, bool $not): string
    {
        $term = $values[0] ?? '';

        return parent::compileSearchExpr($attribute, [$this->prepareSearchTerm(\is_string($term) ? $term : '')], $not);
    }

    private function prepareSearchTerm(string $term): string
    {
        $exact = \str_starts_with($term, self::SEARCH_QUOTE) && \str_ends_with($term, self::SEARCH_QUOTE);

        $words = \preg_replace(self::SEARCH_SEPARATORS, ' ', $term) ?? '';
        $words = \trim(\preg_replace(self::SEARCH_WHITESPACE, ' ', $words) ?? '');

        if ($exact && $words !== '') {
            return self::SEARCH_QUOTE.$words.self::SEARCH_QUOTE;
        }

        return $words;
    }
}
