<?php

namespace Utopia\Database\Adapter\SQL\Hook\Permission;

use Closure;
use InvalidArgumentException;
use Utopia\Database\Adapter\SQL\Hook\Column\AllowNull;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\PermissionType;
use Utopia\Query\Builder\Condition;
use Utopia\Query\Builder\JoinType;
use Utopia\Query\Hook\Filter as FilterHook;
use Utopia\Query\Hook\Join\Condition as JoinCondition;
use Utopia\Query\Hook\Join\Filter as JoinFilter;

/**
 * SQL read hook that generates permission-checking subquery conditions for document access control.
 *
 * Produces an EXISTS/IN subquery against a permissions side table, filtering documents
 * by the current user's roles, permission type, and optionally specific columns.
 */
readonly class Filter implements FilterHook, JoinFilter
{
    private const string IDENTIFIER_PATTERN = '/^[a-zA-Z_][a-zA-Z0-9_.\-]*$/';

    private const string QUOTED_IDENTIFIER_PATTERN = '/^[a-zA-Z0-9_\-][a-zA-Z0-9_.\-]*$/';

    private const string NO_SEMIJOIN = '/*+ NO_SEMIJOIN() */ ';

    private const string COLLATION_PATTERN = '/^[a-zA-Z_][a-zA-Z0-9_]*$/';

    protected string $documentCollation;

    /**
     * @param  list<string>  $roles
     * @param  Closure(string): string  $permissionsTable  Receives the base table name, returns the permissions table name
     * @param  list<string>|null  $columns  Column names to check permissions for. NULL rows (wildcard) are always included.
     * @param  FilterHook|null  $subqueryFilter  Optional filter applied inside the permissions subquery (e.g. tenant filtering)
     * @param  bool  $semiJoin  Whether the engine may merge the subquery into the outer query as a semi-join; when not, it carries MySQL's NO_SEMIJOIN hint, a comment to engines without optimizer hints
     */
    public function __construct(
        protected array $roles,
        protected Closure $permissionsTable,
        protected string $type = PermissionType::Read->value,
        protected ?array $columns = null,
        protected string $documentColumn = 'id',
        protected string $permissionDocumentColumn = 'document_id',
        protected string $permissionRoleColumn = 'role',
        protected string $permissionTypeColumn = 'type',
        protected string $scopeColumn = 'column',
        protected ?FilterHook $subqueryFilter = null,
        protected string $quoteCharacter = '`',
        protected bool $semiJoin = true,
    ) {
        foreach ([$documentColumn, $permissionDocumentColumn, $permissionRoleColumn, $permissionTypeColumn, $scopeColumn] as $column) {
            if (! \preg_match(self::IDENTIFIER_PATTERN, $column)) {
                throw new InvalidArgumentException('Invalid column name: '.$column);
            }
        }
        $this->documentCollation = '';
    }

    /**
     * Generate a SQL condition that filters documents by permission role membership.
     *
     * @param string $table The base table name being queried
     * @return Condition A condition with an IN subquery against the permissions table
     * @throws DatabaseException If the permissions table name is invalid
     */
    public function filter(string $table): Condition
    {
        if (empty($this->roles)) {
            return new Condition('1 = 0');
        }

        /** @var string $permTable */
        $permTable = ($this->permissionsTable)($table);

        if (! \preg_match(self::QUOTED_IDENTIFIER_PATTERN, $permTable)) {
            throw new DatabaseException('Invalid permissions table name: '.$permTable);
        }

        $quotedPermTable = AllowNull::quote($permTable, $this->quoteCharacter);
        $quotedDocumentColumn = AllowNull::quote($this->documentColumn, $this->quoteCharacter);

        $rolePlaceholders = \implode(', ', \array_fill(0, \count($this->roles), '?'));

        $columnClause = '';
        $columnBindings = [];

        if ($this->columns !== null) {
            if (empty($this->columns)) {
                $columnClause = " AND {$this->scopeColumn} IS NULL";
            } else {
                $colPlaceholders = \implode(', ', \array_fill(0, \count($this->columns), '?'));
                $columnClause = " AND ({$this->scopeColumn} IS NULL OR {$this->scopeColumn} IN ({$colPlaceholders}))";
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

        $hint = $this->semiJoin ? '' : self::NO_SEMIJOIN;

        return new Condition(
            "{$quotedDocumentColumn}{$this->documentCollation} IN (SELECT {$hint}{$this->permissionDocumentColumn} FROM {$quotedPermTable} WHERE {$this->permissionRoleColumn} IN ({$rolePlaceholders}) AND {$this->permissionTypeColumn} = ?{$columnClause}{$subFilterClause})",
            [...$this->roles, $this->type, ...$columnBindings, ...$subFilterBindings],
        );
    }

    /**
     * Compare the document column in the collation of the index that serves it.
     *
     * @throws InvalidArgumentException If the collation name is invalid
     */
    public function collate(string $collation): static
    {
        if (! \preg_match(self::COLLATION_PATTERN, $collation)) {
            throw new InvalidArgumentException('Invalid collation name: '.$collation);
        }

        return clone($this, ['documentCollation' => ' COLLATE '.$collation]);
    }

    public function withoutSemiJoin(): static
    {
        return clone($this, ['semiJoin' => false]);
    }

    /**
     * Per-join-table permission checks are applied via separate Permission\Join hooks
     * registered by the SQL adapter for each joined table. This hook only handles the
     * primary table's WHERE clause, so filterJoin returns null.
     */
    public function filterJoin(string $table, JoinType $joinType): ?JoinCondition
    {
        return null;
    }
}
