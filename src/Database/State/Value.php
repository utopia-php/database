<?php

namespace Utopia\Database\State;

use Swoole\Coroutine;
use Swoole\Coroutine\Context;
use WeakMap;

/**
 * A handle-wide value that a callback can override for its own duration.
 *
 * An override belongs to the coroutine that opened it: that coroutine and the coroutines it starts see it, its
 * siblings and its parent do not. A read takes the current coroutine's innermost override, else the nearest
 * ancestor's, else the handle-wide value. A write changes the current coroutine's innermost override when it has
 * one, else the handle-wide value. Outside a coroutine, overrides form one stack that every coroutine sees as its
 * outermost ancestor.
 *
 * @template T
 */
final class Value
{
    private const string CONTEXT = 'utopia.database.state';

    private static ?bool $coroutines = null;

    /**
     * @var list<T>
     */
    private array $overrides = [];

    private int $open = 0;

    /**
     * @param  T  $value
     */
    public function __construct(private mixed $value)
    {
    }

    /**
     * @return T
     */
    public function get(): mixed
    {
        if ($this->open === 0) {
            return $this->value;
        }

        $coroutine = self::coroutine();
        while ($coroutine > 0) {
            $overrides = $this->overridesOf($coroutine);
            if ($overrides !== []) {
                return $overrides[\count($overrides) - 1];
            }

            $parent = Coroutine::getPcid($coroutine);
            $coroutine = \is_int($parent) ? $parent : -1;
        }

        if ($this->overrides !== []) {
            return $this->overrides[\count($this->overrides) - 1];
        }

        return $this->value;
    }

    /**
     * @param  T  $value
     */
    public function set(mixed $value): void
    {
        $coroutine = self::coroutine();
        if ($coroutine > 0) {
            $overrides = $this->overridesOf($coroutine);
            if ($overrides !== []) {
                \array_pop($overrides);
                $overrides[] = $value;
                $this->store($coroutine, $overrides);

                return;
            }
        } elseif ($this->overrides !== []) {
            \array_pop($this->overrides);
            $this->overrides[] = $value;

            return;
        }

        $this->value = $value;
    }

    /**
     * Run the callback with the value overridden for the calling coroutine and the coroutines it starts.
     *
     * @template R
     *
     * @param  T  $value
     * @param  callable(): R  $callback
     * @return R
     */
    public function with(mixed $value, callable $callback): mixed
    {
        $coroutine = self::coroutine();
        if ($coroutine > 0) {
            $overrides = $this->overridesOf($coroutine);
            $overrides[] = $value;
            $this->store($coroutine, $overrides);
        } else {
            $this->overrides[] = $value;
        }

        $this->open++;

        try {
            return $callback();
        } finally {
            $this->open--;

            if ($coroutine > 0) {
                $overrides = $this->overridesOf($coroutine);
                \array_pop($overrides);
                $this->store($coroutine, $overrides);
            } else {
                \array_pop($this->overrides);
            }
        }
    }

    /**
     * @return list<T>
     */
    private function overridesOf(int $coroutine): array
    {
        /** @var list<T> $overrides */
        $overrides = self::scopes($coroutine)[$this] ?? [];

        return $overrides;
    }

    /**
     * @param  list<T>  $overrides
     */
    private function store(int $coroutine, array $overrides): void
    {
        $scopes = self::scopes($coroutine, create: $overrides !== []);
        if ($scopes === null) {
            return;
        }

        if ($overrides === []) {
            unset($scopes[$this]);
        } else {
            $scopes[$this] = $overrides;
        }
    }

    private static function coroutine(): int
    {
        self::$coroutines ??= \extension_loaded('swoole');
        if (! self::$coroutines) {
            return -1;
        }

        $coroutine = Coroutine::getCid();

        return \is_int($coroutine) ? $coroutine : -1;
    }

    /**
     * @return WeakMap<object, mixed>|null
     */
    private static function scopes(int $coroutine, bool $create = false): ?WeakMap
    {
        $context = Coroutine::getContext($coroutine);
        if (! $context instanceof Context) {
            return null;
        }

        $scopes = $context[self::CONTEXT] ?? null;
        if ($scopes instanceof WeakMap) {
            return $scopes;
        }

        if (! $create) {
            return null;
        }

        $scopes = new WeakMap();
        $context[self::CONTEXT] = $scopes;

        return $scopes;
    }
}
