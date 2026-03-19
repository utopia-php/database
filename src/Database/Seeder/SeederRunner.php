<?php

namespace Utopia\Database\Seeder;

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

        foreach ($this->seeders as $class => $seeder) {
            $this->runWithDependencies($class, $db);
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

    private function runWithDependencies(string $class, Database $db): void
    {
        if (isset($this->executed[$class])) {
            return;
        }

        if (! isset($this->seeders[$class])) {
            throw new \RuntimeException("Seeder '{$class}' is not registered");
        }

        foreach ($this->seeders[$class]->dependencies() as $dep) {
            $this->runWithDependencies($dep, $db);
        }

        $this->seeders[$class]->run($db);
        $this->executed[$class] = true;
    }
}
