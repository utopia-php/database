<?php

namespace Utopia\Database\State;

/**
 * One open override of a {@see Value}, or the write kept by a coroutine that cannot reach the open overrides: the
 * value its owner and the coroutines it starts see, and what the coroutines that inherited it wrote while it is open.
 *
 * @internal
 *
 * @template T
 */
final class Scope
{
    /**
     * @var array<int, T> The values written by coroutines that inherited this override, by coroutine id
     */
    public array $writes = [];

    /**
     * @param  int  $coroutine  The coroutine that opened the override or kept the write, or -1 outside coroutines
     * @param  T  $value
     * @param  Scope<T>|null  $outer  The override it is nested in, opened by the same coroutine
     */
    public function __construct(
        public readonly int $coroutine,
        public mixed $value,
        public readonly ?Scope $outer,
    ) {
    }
}
