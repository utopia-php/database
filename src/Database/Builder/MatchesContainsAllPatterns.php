<?php

namespace Utopia\Database\Builder;

/**
 * containsAll() on an attribute that is not an array matches as 7.x did: each value is a LIKE pattern on the whole
 * value, not a substring, and a row matching any one of them is returned.
 */
trait MatchesContainsAllPatterns
{
    /**
     * @param  array<mixed>  $values
     */
    #[\Override]
    protected function compileContainsAll(string $attribute, array $values, ?string $column = null): string
    {
        $like = $this->getLikeKeyword();
        $parts = [];
        foreach ($values as $value) {
            $this->addBinding($value, $column);
            $parts[] = $attribute.' '.$like.' ?';
        }

        return '('.\implode(' OR ', $parts).')';
    }
}
