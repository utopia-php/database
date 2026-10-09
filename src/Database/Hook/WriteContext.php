<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Builder\Scoping;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Query\Builder;
use Utopia\Query\Builder\Statement;

/**
 * What a {@see Write} hook writes its own rows through: builders and statements on the adapter's connection, in the
 * write that called the hook.
 */
interface WriteContext
{
    /**
     * A builder on the adapter's connection. Its from() takes a table as {@see self::rawTable()} does, to read or
     * delete its rows, limited to the adapter's tenant when tables are shared, and the builder then reads that table
     * only. Its into() and, on a builder whose from() has named no table, fromTable() take the name rawTable()
     * returns and limit it to no tenant, for inserting into, or reaching every tenant of, the table.
     */
    public function builder(): Builder&Scoping;

    /**
     * The name the table is stored under.
     */
    public function rawTable(string $table): string;

    /**
     * Run a statement; the event names it to the adapter's transforms and timeouts.
     */
    public function run(Statement $statement, Event $event): bool;

    /**
     * Run a statement and return the rows it reads.
     *
     * @return list<array<string, mixed>>
     */
    public function fetch(Statement $statement, Event $event): array;

    /**
     * The row as every registered write hook decorates a row written for the document.
     *
     * @param  array<string, mixed>  $row
     * @return array<string, mixed>
     */
    public function decorateRow(array $row, Document $document): array;

    /**
     * Whether the update keeps the document's permissions, so its permission rows need no change.
     */
    public function skipPermissions(Document $document): bool;

    /**
     * Whether the write skips the documents already stored, so a row it repeats is ignored rather than rejected.
     */
    public function ignoreDuplicates(): bool;
}
