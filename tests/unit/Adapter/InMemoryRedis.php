<?php

namespace Tests\Unit\Adapter;

use Redis;

/**
 * A Redis client over PHP arrays: strings, hashes and sets, with pipelines that run each
 * command at once and hand the replies to exec(). Enough of the protocol for the Redis
 * adapter's schema and document paths on a host without a server.
 */
final class InMemoryRedis extends Redis
{
    /**
     * @var array<string, string>
     */
    public array $strings = [];

    /**
     * @var array<string, array<string, string>>
     */
    public array $hashes = [];

    /**
     * @var array<string, array<string, true>>
     */
    public array $sets = [];

    /**
     * @var list<mixed>|null
     */
    private ?array $pipeline = null;

    public function __construct()
    {
    }

    public function exists(mixed $key, mixed ...$other_keys): Redis|int|bool
    {
        $count = 0;
        foreach ([$key, ...$other_keys] as $name) {
            $name = (string) $name;
            if (isset($this->strings[$name]) || isset($this->hashes[$name]) || isset($this->sets[$name])) {
                $count++;
            }
        }

        return $this->reply($count);
    }

    public function sIsMember(string $key, mixed $value): Redis|bool
    {
        return $this->reply(isset($this->sets[$key][(string) $value]));
    }

    /**
     * @param  array<string, mixed>  $fieldvals
     */
    public function hMSet(string $key, array $fieldvals): Redis|bool
    {
        foreach ($fieldvals as $field => $value) {
            $this->hashes[$key][(string) $field] = (string) $value;
        }

        return $this->reply(true);
    }

    public function hSet(string $key, mixed ...$fields_and_vals): Redis|int|false
    {
        for ($i = 0; $i + 1 < \count($fields_and_vals); $i += 2) {
            $this->hashes[$key][(string) $fields_and_vals[$i]] = (string) $fields_and_vals[$i + 1];
        }

        return $this->reply(1);
    }

    public function hGet(string $key, string $member): mixed
    {
        return $this->reply($this->hashes[$key][$member] ?? false);
    }

    public function hGetAll(string $key): Redis|array|false
    {
        return $this->reply($this->hashes[$key] ?? []);
    }

    public function hDel(string $key, string $field, string ...$other_fields): Redis|int|false
    {
        $removed = 0;
        foreach ([$field, ...$other_fields] as $name) {
            if (isset($this->hashes[$key][$name])) {
                unset($this->hashes[$key][$name]);
                $removed++;
            }
        }

        return $this->reply($removed);
    }

    public function del(array|string $key, string ...$other_keys): Redis|int|false
    {
        $removed = 0;
        foreach ([...(\is_array($key) ? $key : [$key]), ...$other_keys] as $name) {
            $name = (string) $name;
            if (isset($this->strings[$name]) || isset($this->hashes[$name]) || isset($this->sets[$name])) {
                $removed++;
            }
            unset($this->strings[$name], $this->hashes[$name], $this->sets[$name]);
        }

        return $this->reply($removed);
    }

    public function sAdd(string $key, mixed $value, mixed ...$other_values): Redis|int|false
    {
        $added = 0;
        foreach ([$value, ...$other_values] as $member) {
            if (! isset($this->sets[$key][(string) $member])) {
                $this->sets[$key][(string) $member] = true;
                $added++;
            }
        }

        return $this->reply($added);
    }

    public function sRem(string $key, mixed $value, mixed ...$other_values): Redis|int|false
    {
        $removed = 0;
        foreach ([$value, ...$other_values] as $member) {
            if (isset($this->sets[$key][(string) $member])) {
                unset($this->sets[$key][(string) $member]);
                $removed++;
            }
        }
        if (($this->sets[$key] ?? null) === []) {
            unset($this->sets[$key]);
        }

        return $this->reply($removed);
    }

    public function sMembers(string $key): Redis|array|false
    {
        return $this->reply(\array_map('strval', \array_keys($this->sets[$key] ?? [])));
    }

    public function sCard(string $key): Redis|int|false
    {
        return $this->reply(\count($this->sets[$key] ?? []));
    }

    public function sUnion(string $key, string ...$other_keys): Redis|array|false
    {
        $members = [];
        foreach ([$key, ...$other_keys] as $name) {
            foreach (\array_keys($this->sets[$name] ?? []) as $member) {
                $members[(string) $member] = true;
            }
        }

        return $this->reply(\array_map('strval', \array_keys($members)));
    }

    public function get(string $key): mixed
    {
        return $this->reply($this->strings[$key] ?? false);
    }

    public function set(string $key, mixed $value, mixed $options = null): Redis|string|bool
    {
        $this->strings[$key] = (string) $value;

        return $this->reply(true);
    }

    /**
     * @param  array<string>  $keys
     */
    public function mGet(array $keys): Redis|array|false
    {
        return $this->reply(\array_map(fn (string $key): string|false => $this->strings[$key] ?? false, \array_values($keys)));
    }

    public function incr(string $key, int $by = 1): Redis|int|false
    {
        $value = (int) ($this->strings[$key] ?? 0) + $by;
        $this->strings[$key] = (string) $value;

        return $this->reply($value);
    }

    public function type(string $key): Redis|int|false
    {
        return $this->reply(match (true) {
            isset($this->strings[$key]) => Redis::REDIS_STRING,
            isset($this->hashes[$key]) => Redis::REDIS_HASH,
            isset($this->sets[$key]) => Redis::REDIS_SET,
            default => Redis::REDIS_NOT_FOUND,
        });
    }

    public function scan(string|int|null &$iterator, ?string $pattern = null, int $count = 0, ?string $type = null): array|false
    {
        $iterator = 0;
        $keys = \array_keys($this->strings + $this->hashes + $this->sets);

        return \array_values(\array_filter(
            \array_map('strval', $keys),
            static fn (string $key): bool => $pattern === null || \fnmatch($pattern, $key),
        ));
    }

    public function multi(int $value = Redis::MULTI): Redis|bool
    {
        $this->pipeline = [];

        return $this;
    }

    public function exec(): Redis|array|false
    {
        $replies = $this->pipeline ?? [];
        $this->pipeline = null;

        return $replies;
    }

    public function discard(): Redis|bool
    {
        $this->pipeline = null;

        return true;
    }

    private function reply(mixed $value): mixed
    {
        if ($this->pipeline === null) {
            return $value;
        }

        $this->pipeline[] = $value;

        return $this;
    }
}
