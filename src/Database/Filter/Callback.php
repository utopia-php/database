<?php

namespace Utopia\Database\Filter;

use Closure;
use ReflectionFunction;

final readonly class Callback implements Signed
{
    public function __construct(
        private string $name,
        private Closure $encode,
        private Closure $decode,
    ) {
    }

    #[\Override]
    public function name(): string
    {
        return $this->name;
    }

    #[\Override]
    public function encode(mixed $value): mixed
    {
        return ($this->encode)($value);
    }

    #[\Override]
    public function decode(mixed $value): mixed
    {
        return ($this->decode)($value);
    }

    /**
     * Where the two closures are declared, so cached documents decoded by other closures are told apart while
     * every process declaring the same ones shares them.
     */
    #[\Override]
    public function signature(): string
    {
        return self::declaration($this->encode).':'.self::declaration($this->decode);
    }

    private static function declaration(Closure $closure): string
    {
        $reflection = new ReflectionFunction($closure);
        $scope = $reflection->getClosureScopeClass()?->getName() ?? '';

        return ($reflection->getFileName() ?: $scope.'::'.$reflection->getName()).':'.$reflection->getStartLine();
    }
}
