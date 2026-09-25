<?php

namespace Utopia\Database\Hook;

use Utopia\Query\Builder\JoinType;
use Utopia\Query\Method;
use Utopia\Query\Query;

/**
 * The joins of one read, in the order the builder pairs them.
 *
 * Inner and left joins carry a table's conditions in their own ON. Right, full outer and cross
 * joins leave them in WHERE, which runs only after every join has paired its rows, and right and
 * full outer joins can leave the tables before them missing from a row.
 */
final readonly class JoinChain
{
    /**
     * @param array<string, JoinType> $joins Each joined table's alias and how it is joined, in order
     */
    public function __construct(
        private array $joins = [],
    ) {
    }

    /**
     * @param array<Query> $queries The read's queries; joins are keyed the way the builder hands
     *                              them to join filters: by alias, or by table without one
     */
    public static function fromQueries(array $queries): self
    {
        $joins = [];
        foreach ($queries as $query) {
            $type = match ($query->getMethod()) {
                Method::Join => JoinType::Inner,
                Method::LeftJoin => JoinType::Left,
                Method::RightJoin => JoinType::Right,
                Method::FullOuterJoin => JoinType::FullOuter,
                Method::CrossJoin => JoinType::Cross,
                Method::NaturalJoin => JoinType::Natural,
                default => null,
            };

            if ($type !== null) {
                $alias = $query->getJoinAlias();
                $joins[$alias !== '' ? $alias : $query->getAttribute()] = $type;
            }
        }

        return new self($joins);
    }

    public function has(JoinType $type): bool
    {
        return \in_array($type, $this->joins, true);
    }

    /**
     * Whether a right or full outer join can leave a table missing from a row, so that a condition
     * in WHERE has to let such rows through.
     */
    public function hasPreservingOuterJoin(): bool
    {
        return $this->has(JoinType::Right) || $this->has(JoinType::FullOuter);
    }

    /**
     * The tables joined before $alias whose conditions sit in WHERE: the ones joined right, full
     * outer or cross. A table joined inner or left already meets its conditions in its own ON.
     *
     * @return list<string>
     */
    public function preceding(string $alias): array
    {
        $preceding = [];
        foreach ($this->joins as $joined => $type) {
            if ($joined === $alias) {
                return $preceding;
            }

            if ($type !== JoinType::Inner && $type !== JoinType::Left) {
                $preceding[] = $joined;
            }
        }

        return [];
    }
}
