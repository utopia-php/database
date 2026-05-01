<?php

namespace Utopia\Database\Traits;

use Throwable;
use Utopia\Async\Parallel;
use Utopia\Async\Promise;

/**
 * Provides concurrent and parallel execution primitives for I/O-bound and CPU-bound tasks.
 */
trait Async
{
    /**
     * Run I/O-bound tasks concurrently via coroutines (Promise).
     *
     * Coroutines yield on I/O (database queries, network calls), allowing
     * other tasks to progress on the same thread. No benefit for CPU-bound work.
     *
     * @param  array<callable>  $tasks
     * @return array<mixed> Results in same order as input tasks
     */
    protected function promise(array $tasks): array
    {
        if (\count($tasks) <= 1) {
            return \array_map(fn (callable $task) => $task(), $tasks);
        }

        /** @var array<mixed> $results */
        $results = Promise::map($tasks)->await();

        return $results;
    }

    /**
     * Like promise() but settles all tasks regardless of individual failures.
     *
     * Returns null for failed tasks instead of throwing.
     * Useful for write hooks where one failure shouldn't block others.
     *
     * @param  array<callable>  $tasks
     * @return array<mixed> Results in same order as input tasks (null for failed tasks)
     */
    protected function promiseSettled(array $tasks): array
    {
        if (\count($tasks) <= 1) {
            return \array_map(function (callable $task) {
                try {
                    return $task();
                } catch (Throwable) {
                    return;
                }
            }, $tasks);
        }

        $promises = \array_map(
            fn (callable $task) => Promise::async($task),
            $tasks
        );

        /** @var array<array{status: string, value?: mixed, reason?: mixed}> $settlements */
        $settlements = Promise::allSettled($promises)->await();

        return \array_map(
            fn (array $s) => $s['status'] === 'fulfilled' ? ($s['value'] ?? null) : null,
            $settlements
        );
    }

    /**
     * Run CPU-bound tasks in parallel via threads/processes (Parallel).
     *
     * Tasks execute on separate CPU cores for true parallelism.
     * Falls back to sequential execution when no parallel runtime is available.
     *
     * @param  array<callable>  $tasks
     * @return array<mixed> Results in same order as input tasks
     */
    protected function parallel(array $tasks): array
    {
        if (\count($tasks) <= 1) {
            return \array_map(fn (callable $task) => $task(), $tasks);
        }

        /** @var array<mixed> $results */
        $results = Parallel::all($tasks);

        return $results;
    }

    /**
     * Map a callback over items in parallel via threads/processes.
     *
     * More ergonomic than parallel() for batch transformations.
     * Automatically chunks work across available CPU cores.
     *
     * @param  array<mixed>  $items
     * @param  callable  $callback  fn($item, $index) => mixed
     * @return array<mixed> Results in same order as input items
     */
    protected function parallelMap(array $items, callable $callback): array
    {
        if (\count($items) <= 1) {
            return \array_map($callback, $items, \array_keys($items));
        }

        /** @var array<mixed> $results */
        $results = Parallel::map($items, $callback);

        return $results;
    }
}
