<?php

namespace Utopia\Database\Hook;

use Closure;
use InvalidArgumentException;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Filter;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;
use Utopia\Query\Hook\Join\Placement;

/**
 * SQL read hook that generates permission-checking subquery conditions for document access control.
 *
 * Produces an EXISTS/IN subquery against a permissions side table, filtering documents
 * by the current user's roles, permission type, and optionally specific columns.
 */
class PermissionFilter implements Filter, JoinFilter
{
    private const IDENTIFIER_PATTERN = '/^[a-zA-Z_][a-zA-Z0-9_.\-]*$/';

    /**
     * @param  list<string>  $roles
     * @param  Closure(string): string  $permissionsTable  Receives the base table name, returns the permissions table name
     * @param  list<string>|null  $columns  Column names to check permissions for. NULL rows (wildcard) are always included.
     * @param  Filter|null  $subqueryFilter  Optional filter applied inside the permissions subquery (e.g. tenant filtering)
     */
    public function __construct(
        protected array $roles,
        protected Closure $permissionsTable,
        protected string $type = 'read',
        protected ?array $columns = null,
        protected string $documentColumn = 'id',
        protected string $permDocumentColumn = 'document_id',
        protected string $permRoleColumn = 'role',
        protected string $permTypeColumn = 'type',
        protected string $permColumnColumn = 'column',
        protected ?Filter $subqueryFilter = null,
        protected string $quoteChar = '`',
    ) {
        foreach ([$documentColumn, $permDocumentColumn, $permRoleColumn, $permTypeColumn, $permColumnColumn] as $col) {
            if (! \preg_match(self::IDENTIFIER_PATTERN, $col)) {
                throw new InvalidArgumentException('Invalid column name: '.$col);
            }
        }
    }

    /**
     * Generate a SQL condition that filters documents by permission role membership.
     *
     * @param string $table The base table name being queried
     * @return Condition A condition with an IN subquery against the permissions table
     * @throws InvalidArgumentException If the permissions table name is invalid
     */
    public function filter(string $table): Condition
    {
        if (empty($this->roles)) {
            return new Condition('1 = 0');
        }

        /** @var string $permTable */
        $permTable = ($this->permissionsTable)($table);

        if (! \preg_match(self::IDENTIFIER_PATTERN, $permTable)) {
            throw new InvalidArgumentException('Invalid permissions table name: '.$permTable);
        }

        $quotedPermTable = $this->quoteTableIdentifier($permTable);

        $rolePlaceholders = \implode(', ', \array_fill(0, \count($this->roles), '?'));

        $columnClause = '';
        $columnBindings = [];

        if ($this->columns !== null) {
            if (empty($this->columns)) {
                $columnClause = " AND {$this->permColumnColumn} IS NULL";
            } else {
                $colPlaceholders = \implode(', ', \array_fill(0, \count($this->columns), '?'));
                $columnClause = " AND ({$this->permColumnColumn} IS NULL OR {$this->permColumnColumn} IN ({$colPlaceholders}))";
                $columnBindings = $this->columns;
            }
        }

        $subFilterClause = '';
        $subFilterBindings = [];
        if ($this->subqueryFilter !== null) {
            $subCondition = $this->subqueryFilter->filter($permTable);
            $subFilterClause = ' AND '.$subCondition->expression;
            $subFilterBindings = $subCondition->bindings;
        }

        return new Condition(
            "{$this->documentColumn} IN (SELECT DISTINCT {$this->permDocumentColumn} FROM {$quotedPermTable} WHERE {$this->permRoleColumn} IN ({$rolePlaceholders}) AND {$this->permTypeColumn} = ?{$columnClause}{$subFilterClause})",
            [...$this->roles, $this->type, ...$columnBindings, ...$subFilterBindings],
        );
    }

    /**
     * Generate a permission filter condition for JOIN operations, placed on ON or WHERE depending on join type.
     *
     * @param string $table The base table name being joined
     * @param JoinType $joinType The type of join being performed
     * @return JoinCondition|null The join condition with appropriate placement, or null if not applicable
     */
    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        $condition = $this->filter($table);

        $placement = match ($joinType) {
            JoinType::Left, JoinType::Right => Placement::On,
            default => Placement::Where,
        };

        return new JoinCondition($condition, $placement);
    }

    private function quoteTableIdentifier(string $table): string
    {
        $q = $this->quoteChar;
        $parts = \explode('.', $table);
        $quoted = \array_map(fn (string $part): string => $q.\str_replace($q, $q.$q, $part).$q, $parts);

        return \implode('.', $quoted);
    }
}
