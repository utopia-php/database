<?php

/**
 * @var CLI $cli
 */
global $cli;

use Utopia\CLI\CLI;
use Utopia\Console;
use Utopia\Database\Database;
use Utopia\Database\Migration\Migration;
use Utopia\Database\Migration\MigrationGenerator;
use Utopia\Database\Migration\MigrationRunner;
use Utopia\Validator\Boolean;
use Utopia\Validator\Integer;
use Utopia\Validator\Text;

/**
 * @Example
 * docker compose exec tests bin/cli migrate --adapter=mysql --name=testing --path=migrations
 * docker compose exec tests bin/cli migrate:rollback --adapter=mysql --name=testing --path=migrations --steps=1
 * docker compose exec tests bin/cli migrate:status --adapter=mysql --name=testing --path=migrations
 * docker compose exec tests bin/cli migrate:fresh --adapter=mysql --name=testing --path=migrations
 * docker compose exec tests bin/cli migrate:generate --name=add_users_table
 */

$cli
    ->task('migrate')
    ->desc('Run pending database migrations')
    ->param('path', 'migrations', new Text(0), 'Path to migration files', true)
    ->param('adapter', '', new Text(0), 'Database adapter')
    ->param('name', '', new Text(0), 'Database name')
    ->param('namespace', '_ns', new Text(0), 'Database namespace', true)
    ->param('sharedTables', false, new Boolean(true), 'Whether to use shared tables', true)
    ->inject('database')
    ->action(function (string $path, string $adapter, string $name, string $namespace, bool $sharedTables, callable $database) {
        $migrations = loadMigrations($path);

        if ($migrations === []) {
            Console::warning('No migration files found in: ' . $path);

            return;
        }

        Console::info('Running migrations...');

        $db = $database($adapter, $name, $namespace, $sharedTables);
        if (! $db instanceof Database) {
            throw new \RuntimeException('The database resource must return a Database instance.');
        }
        $runner = new MigrationRunner($db);
        $count = $runner->migrate($migrations);

        Console::success("Ran {$count} migration(s).");
    });

$cli
    ->task('migrate:rollback')
    ->desc('Rollback the last batch of migrations')
    ->param('path', 'migrations', new Text(0), 'Path to migration files', true)
    ->param('steps', 1, new Integer(true), 'Number of batches to rollback', true)
    ->param('adapter', '', new Text(0), 'Database adapter')
    ->param('name', '', new Text(0), 'Database name')
    ->param('namespace', '_ns', new Text(0), 'Database namespace', true)
    ->param('sharedTables', false, new Boolean(true), 'Whether to use shared tables', true)
    ->inject('database')
    ->action(function (string $path, int $steps, string $adapter, string $name, string $namespace, bool $sharedTables, callable $database) {
        $migrations = loadMigrations($path);
        $db = $database($adapter, $name, $namespace, $sharedTables);
        if (! $db instanceof Database) {
            throw new \RuntimeException('The database resource must return a Database instance.');
        }
        $runner = new MigrationRunner($db);
        $count = $runner->rollback($migrations, $steps);

        Console::success("Rolled back {$count} migration(s).");
    });

$cli
    ->task('migrate:status')
    ->desc('Show the status of all migrations')
    ->param('path', 'migrations', new Text(0), 'Path to migration files', true)
    ->param('adapter', '', new Text(0), 'Database adapter')
    ->param('name', '', new Text(0), 'Database name')
    ->param('namespace', '_ns', new Text(0), 'Database namespace', true)
    ->param('sharedTables', false, new Boolean(true), 'Whether to use shared tables', true)
    ->inject('database')
    ->action(function (string $path, string $adapter, string $name, string $namespace, bool $sharedTables, callable $database) {
        $migrations = loadMigrations($path);
        $db = $database($adapter, $name, $namespace, $sharedTables);
        if (! $db instanceof Database) {
            throw new \RuntimeException('The database resource must return a Database instance.');
        }
        $runner = new MigrationRunner($db);
        $status = $runner->status($migrations);

        Console::info(\str_pad('Version', 20) . \str_pad('Name', 40) . 'Applied');
        Console::info(\str_repeat('-', 70));

        foreach ($status as $entry) {
            $applied = $entry['applied'] ? 'Yes' : 'No';
            Console::log(\str_pad($entry['version'], 20) . \str_pad($entry['name'], 40) . $applied);
        }
    });

$cli
    ->task('migrate:fresh')
    ->desc('Drop all collections and re-run all migrations')
    ->param('path', 'migrations', new Text(0), 'Path to migration files', true)
    ->param('adapter', '', new Text(0), 'Database adapter')
    ->param('name', '', new Text(0), 'Database name')
    ->param('namespace', '_ns', new Text(0), 'Database namespace', true)
    ->param('sharedTables', false, new Boolean(true), 'Whether to use shared tables', true)
    ->inject('database')
    ->action(function (string $path, string $adapter, string $name, string $namespace, bool $sharedTables, callable $database) {
        $migrations = loadMigrations($path);
        $db = $database($adapter, $name, $namespace, $sharedTables);
        if (! $db instanceof Database) {
            throw new \RuntimeException('The database resource must return a Database instance.');
        }
        $runner = new MigrationRunner($db);

        Console::warning('Dropping all collections and re-migrating...');
        $count = $runner->fresh($migrations);

        Console::success("Fresh migration complete. Ran {$count} migration(s).");
    });

$cli
    ->task('migrate:generate')
    ->desc('Generate an empty migration file')
    ->param('name', '', new Text(0), 'Migration name (e.g. add_users_table)')
    ->param('path', 'migrations', new Text(0), 'Output directory', true)
    ->action(function (string $name, string $path) {
        $timestamp = \date('YmdHis');
        $className = 'V' . $timestamp . '_' . \str_replace(' ', '', \ucwords(\str_replace('_', ' ', $name)));

        $generator = new MigrationGenerator();
        $content = $generator->generateEmpty($className);

        if (! \is_dir($path)) {
            \mkdir($path, 0755, true);
        }

        $filePath = $path . '/' . $className . '.php';
        \file_put_contents($filePath, $content);

        Console::success("Created migration: {$filePath}");
    });

/**
 * @return array<Migration>
 */
function loadMigrations(string $path): array
{
    if (! \is_dir($path)) {
        return [];
    }

    $migrations = [];
    $files = \glob($path . '/*.php');

    if ($files === false) {
        return [];
    }

    foreach ($files as $file) {
        require_once $file;

        $className = \pathinfo($file, PATHINFO_FILENAME);
        if (\class_exists($className) && \is_subclass_of($className, Migration::class)) {
            $migrations[] = new $className();
        }
    }

    return $migrations;
}
