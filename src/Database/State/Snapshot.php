<?php

namespace Utopia\Database\State;

/**
 * The scoped state a database handle's reads depend on, as one coroutine sees it. Taken with
 * Database::snapshot() and applied with Database::withSnapshot(), it lets work started in another coroutine run
 * under its caller's state, however long the caller keeps that state.
 */
final readonly class Snapshot
{
    /**
     * @param  bool  $authorization  Whether authorization checks apply
     * @param  bool  $relationships  Whether relationships are populated and written
     * @param  bool  $existCheck  Whether related documents must exist before they are linked
     * @param  bool  $population  Whether a relationship population is already running
     * @param  bool  $silenced  Whether every lifecycle hook is silenced
     * @param  array<string, true>  $silencedListeners  Names of the silenced named lifecycle hooks
     */
    public function __construct(
        public bool $authorization,
        public bool $relationships,
        public bool $existCheck,
        public bool $population,
        public bool $silenced,
        public array $silencedListeners,
    ) {
    }
}
