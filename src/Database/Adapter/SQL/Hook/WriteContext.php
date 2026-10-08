<?php

namespace Utopia\Database\Adapter\SQL\Hook;

use Closure;
use PDOStatement;
use Swoole\Database\PDOStatementProxy;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\WriteContext as Context;
use Utopia\Database\PDOStatement as DatabasePDOStatement;
use Utopia\Query\Builder;
use Utopia\Query\Builder\Statement;

/**
 * The SQL adapter's write context: every call goes through the adapter that built it, on its connection.
 *
 * @internal Built by {@see \Utopia\Database\Adapter\SQL} for its write hooks.
 */
final readonly class WriteContext implements Context
{
    /**
     * @param  Closure(string): Builder  $builder
     * @param  Closure(): Builder  $rawBuilder
     * @param  Closure(string): string  $rawTable
     * @param  Closure(Statement, Event): (PDOStatement|DatabasePDOStatement|PDOStatementProxy)  $prepare
     * @param  Closure(PDOStatement|DatabasePDOStatement|PDOStatementProxy): bool  $execute
     * @param  Closure(array<string, mixed>, Document): array<string, mixed>  $decorateRow
     * @param  array<string, true>  $skipPermissions  Ids of the documents whose permissions the write keeps
     */
    public function __construct(
        private Closure $builder,
        private Closure $rawBuilder,
        private Closure $rawTable,
        private Closure $prepare,
        private Closure $execute,
        private Closure $decorateRow,
        private bool $ignoreDuplicates,
        private array $skipPermissions = [],
    ) {
    }

    #[\Override]
    public function builder(string $table): Builder
    {
        return ($this->builder)($table);
    }

    #[\Override]
    public function rawBuilder(): Builder
    {
        return ($this->rawBuilder)();
    }

    #[\Override]
    public function rawTable(string $table): string
    {
        return ($this->rawTable)($table);
    }

    #[\Override]
    public function run(Statement $statement, Event $event): bool
    {
        return ($this->execute)(($this->prepare)($statement, $event));
    }

    #[\Override]
    public function fetch(Statement $statement, Event $event): array
    {
        $prepared = ($this->prepare)($statement, $event);
        ($this->execute)($prepared);
        /** @var list<array<string, mixed>> $rows */
        $rows = $prepared->fetchAll();
        $prepared->closeCursor();

        return $rows;
    }

    #[\Override]
    public function decorateRow(array $row, Document $document): array
    {
        return ($this->decorateRow)($row, $document);
    }

    #[\Override]
    public function skipPermissions(Document $document): bool
    {
        return isset($this->skipPermissions[$document->getId()]);
    }

    #[\Override]
    public function ignoreDuplicates(): bool
    {
        return $this->ignoreDuplicates;
    }
}
