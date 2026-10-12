<?php

namespace Utopia\Database\Filter;

use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Filter;

/**
 * The filters of the Database handles it is set on. A filter registered later reaches every one of them, and one
 * registered under a name already taken replaces the earlier one.
 *
 * Cache keys tell codecs apart by {@see Signed::signature()} or, for any other codec, by class alone.
 */
final class Registry
{
    /** @var array<string, Codec> */
    private array $codecs = [];

    /** @var array<string, string> */
    private array $signatures = [];

    /**
     * @throws DuplicateException When the codec is named after a built-in filter
     */
    public function register(Codec $codec): static
    {
        $name = $codec->name();

        if (Filter::tryFrom($name) !== null) {
            throw new DuplicateException("Filter \"{$name}\" collides with the built-in filter of the same name");
        }

        $this->codecs[$name] = $codec;
        $this->signatures[$name] = $codec instanceof Signed ? $codec->signature() : $codec::class;

        return $this;
    }

    public function get(string $name): ?Codec
    {
        return $this->codecs[$name] ?? null;
    }

    public function has(string $name): bool
    {
        return isset($this->codecs[$name]);
    }

    /**
     * What tells each registered codec apart in a cache key, by name.
     *
     * @internal
     *
     * @return array<string, string>
     */
    public function signatures(): array
    {
        return $this->signatures;
    }
}
