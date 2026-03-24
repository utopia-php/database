<?php

namespace Utopia\Database\Seeder;

use Utopia\Async\Promise;
use Utopia\Database\Database;

class SeederRunner
{
    /** @var array<class-string<Seeder>, Seeder> */
    private array $seeders = [];

    /** @var array<string, bool> */
    private array $executed = [];

    public function register(Seeder $seeder): void
    {
        $this->seeders[$seeder::class] = $seeder;
    }

    public function run(Database $db): void
    {
        $this->executed = [];
        $remaining = $this->seeders;

        while ($remaining !== []) {
            $ready = [];
            foreach ($remaining as $class => $seeder) {
                $deps = $seeder->dependencies();
                $allDepsResolved = true;
                foreach ($deps as $dep) {
                    if (! isset($this->executed[$dep])) {
                        $allDepsResolved = false;
                        break;
                    }
                }
                if ($allDepsResolved) {
                    $ready[$class] = $seeder;
                }
            }

            if ($ready === []) {
                $unresolved = \implode(', ', \array_keys($remaining));
                throw new \RuntimeException("Circular dependency detected in seeders: {$unresolved}");
            }

            if (\count($ready) > 1) {
                $tasks = [];
                foreach ($ready as $class => $seeder) {
                    $tasks[] = function () use ($seeder, $db): void {
                        $seeder->run($db);
                    };
                }
                Promise::map($tasks)->await();
            } else {
                foreach ($ready as $seeder) {
                    $seeder->run($db);
                }
            }

            foreach ($ready as $class => $seeder) {
                $this->executed[$class] = true;
                unset($remaining[$class]);
            }
        }
    }

    /**
     * @return array<string, bool>
     */
    public function getExecuted(): array
    {
        return $this->executed;
    }

    public function reset(): void
    {
        $this->executed = [];
    }
}
