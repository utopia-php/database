<?php

namespace Utopia\Database\State;

use Swoole\Coroutine;

/**
 * A handle-wide value that a callback can override for its own duration.
 *
 * An override belongs to the coroutine that opened it: that coroutine and the coroutines it starts see it, its
 * siblings and its parent do not. Outside a coroutine, overrides form one stack that every coroutine sees as its
 * outermost ancestor. A read takes the current coroutine's innermost override, else the nearest ancestor's, else the
 * handle-wide value.
 *
 * A write inside an override changes what the writing coroutine, and the coroutines it starts after, see until that
 * override ends: the override itself when the writer opened it, else the writer's own view of it. A write outside
 * every override changes the handle-wide value.
 *
 * @template T
 */
final class Value
{
    private const int OUTSIDE = -1;

    private static ?bool $coroutines = null;

    /**
     * @var array<int, Scope<T>> The innermost open override of each coroutine that has one, by coroutine id
     */
    private array $scopes = [];

    private int $open = 0;

    /**
     * @param  T  $value
     */
    public function __construct(private mixed $value)
    {
        self::$coroutines ??= \extension_loaded('swoole');
    }

    /**
     * @return T
     */
    public function get(): mixed
    {
        if ($this->open === 0) {
            return $this->value;
        }

        $reader = self::$coroutines ? Coroutine::getCid() : self::OUTSIDE;
        $coroutine = $reader;
        while (! isset($this->scopes[$coroutine])) {
            if ($coroutine === self::OUTSIDE) {
                return $this->value;
            }

            $parent = Coroutine::getPcid($coroutine);
            $coroutine = $parent === false ? self::OUTSIDE : $parent;
        }

        $scope = $this->scopes[$coroutine];
        if ($scope->writes === [] || $coroutine === $reader) {
            return $scope->value;
        }

        return self::inherited($scope, $reader);
    }

    /**
     * @param  T  $value
     */
    public function set(mixed $value): void
    {
        if ($this->open === 0) {
            $this->value = $value;

            return;
        }

        $writer = self::coroutine();
        $coroutine = $writer;
        while (! isset($this->scopes[$coroutine])) {
            if ($coroutine === self::OUTSIDE) {
                $this->value = $value;

                return;
            }

            $coroutine = self::parent($coroutine);
        }

        $scope = $this->scopes[$coroutine];
        if ($coroutine === $writer) {
            $scope->value = $value;

            return;
        }

        if (! \array_key_exists($writer, $scope->writes)) {
            Coroutine::defer(static function () use ($scope, $writer): void {
                unset($scope->writes[$writer]);
            });
        }

        $scope->writes[$writer] = $value;
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
        $scope = new Scope($coroutine, $value, $this->scopes[$coroutine] ?? null);
        $this->scopes[$coroutine] = $scope;
        $this->open++;

        try {
            return $callback();
        } finally {
            $this->open--;

            if ($scope->outer === null) {
                unset($this->scopes[$coroutine]);
            } else {
                $this->scopes[$coroutine] = $scope->outer;
            }
        }
    }

    /**
     * The value the reader sees through an override it inherited: the nearest write by the reader or an ancestor
     * below the override's owner, else the override's value.
     *
     * @param  Scope<T>  $scope
     * @return T
     */
    private static function inherited(Scope $scope, int $reader): mixed
    {
        for ($coroutine = $reader; $coroutine !== $scope->coroutine; $coroutine = self::parent($coroutine)) {
            if (\array_key_exists($coroutine, $scope->writes)) {
                return $scope->writes[$coroutine];
            }
        }

        return $scope->value;
    }

    private static function coroutine(): int
    {
        return self::$coroutines ? Coroutine::getCid() : self::OUTSIDE;
    }

    private static function parent(int $coroutine): int
    {
        $parent = Coroutine::getPcid($coroutine);

        return $parent === false ? self::OUTSIDE : $parent;
    }
}
