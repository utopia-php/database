<?php

namespace Utopia\Database\State;

use DateTime;

/**
 * The scoped state a database handle's reads depend on, as one coroutine sees it. Taken with
 * Database::snapshot() and applied with Database::withSnapshot(), it lets work started in another coroutine run
 * under its caller's state, however long the caller keeps that state.
 */
final readonly class Snapshot
{
    /**
     * @param  bool  $authorization  Whether authorization checks apply
     * @param  array<string>  $roles  The roles authorization checks against
     * @param  bool  $relationships  Whether relationships are populated and written
     * @param  bool  $existCheck  Whether related documents must exist before they are linked
     * @param  bool  $population  Whether a relationship population is already running
     * @param  bool  $silenced  Whether every lifecycle hook is silenced
     * @param  array<string, true>  $silencedListeners  Names of the silenced named lifecycle hooks
     * @param  int|string|null  $tenant  The tenant reads and writes use
     * @param  bool  $filters  Whether attribute filters apply
     * @param  array<string, bool>|null  $disabledFilters  Names of the attribute filters that do not apply
     * @param  bool  $validation  Whether documents and queries are validated
     * @param  bool  $preserveDates  Whether writes keep the dates they are given
     * @param  bool  $preserveSequence  Whether writes keep the sequences they are given
     * @param  bool  $ignoreDuplicates  Whether creating a document that exists is skipped instead of failing
     * @param  DateTime|null  $requestTimestamp  The time an update conflicts after
     */
    public function __construct(
        public bool $authorization,
        public array $roles,
        public bool $relationships,
        public bool $existCheck,
        public bool $population,
        public bool $silenced,
        public array $silencedListeners,
        public int|string|null $tenant,
        public bool $filters,
        public ?array $disabledFilters,
        public bool $validation,
        public bool $preserveDates,
        public bool $preserveSequence,
        public bool $ignoreDuplicates,
        public ?DateTime $requestTimestamp,
    ) {
    }
}
