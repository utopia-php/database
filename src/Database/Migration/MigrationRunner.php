<?php

namespace Utopia\Database\Migration;

use Utopia\Database\Database;

class MigrationRunner
{
    private Database $db;

    private MigrationTracker $tracker;

    public function __construct(Database $db, ?MigrationTracker $tracker = null)
    {
        $this->db = $db;
        $this->tracker = $tracker ?? new MigrationTracker($db);
    }

    /**
     * @param  array<Migration>  $migrations
     */
    public function migrate(array $migrations): int
    {
        $this->tracker->setup();
        $executed = $this->tracker->getAppliedVersions();
        $batch = $this->tracker->getLastBatch() + 1;

        $pending = \array_filter(
            $migrations,
            fn (Migration $m) => ! \in_array($m->version(), $executed, true)
        );

        \usort($pending, fn (Migration $a, Migration $b) => \strcmp($a->version(), $b->version()));

        $count = 0;

        foreach ($pending as $migration) {
            $this->db->withTransaction(function () use ($migration, $batch): void {
                $migration->up($this->db);
                $this->tracker->markApplied($migration->version(), $migration->name(), $batch);
            });
            $count++;
        }

        return $count;
    }

    /**
     * @param  array<Migration>  $migrations
     */
    public function rollback(array $migrations, int $steps = 1): int
    {
        $this->tracker->setup();
        $lastBatch = $this->tracker->getLastBatch();
        $count = 0;

        $migrationsByVersion = [];
        foreach ($migrations as $migration) {
            $migrationsByVersion[$migration->version()] = $migration;
        }

        for ($batch = $lastBatch; $batch > $lastBatch - $steps && $batch > 0; $batch--) {
            $applied = $this->tracker->getByBatch($batch);

            foreach ($applied as $doc) {
                $version = $doc->getAttribute('version', '');

                if (isset($migrationsByVersion[$version])) {
                    $this->db->withTransaction(function () use ($migrationsByVersion, $version): void {
                        $migrationsByVersion[$version]->down($this->db);
                        $this->tracker->markRolledBack($version);
                    });
                    $count++;
                }
            }
        }

        return $count;
    }

    /**
     * @param  array<Migration>  $migrations
     * @return array<array{version: string, name: string, applied: bool}>
     */
    public function status(array $migrations): array
    {
        $this->tracker->setup();
        $executed = $this->tracker->getAppliedVersions();
        $status = [];

        \usort($migrations, fn (Migration $a, Migration $b) => \strcmp($a->version(), $b->version()));

        foreach ($migrations as $migration) {
            $status[] = [
                'version' => $migration->version(),
                'name' => $migration->name(),
                'applied' => \in_array($migration->version(), $executed, true),
            ];
        }

        return $status;
    }

    /**
     * @param  array<Migration>  $migrations
     */
    public function fresh(array $migrations): int
    {
        $collections = $this->db->listCollections();

        foreach ($collections as $collection) {
            $id = $collection->getId();
            if ($id !== '_metadata' && $id !== '') {
                try {
                    $this->db->deleteCollection($id);
                } catch (\Throwable) {
                }
            }
        }

        return $this->migrate($migrations);
    }

    public function getTracker(): MigrationTracker
    {
        return $this->tracker;
    }
}
